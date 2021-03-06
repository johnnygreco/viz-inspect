#!/usr/bin/env python
# -*- coding: utf-8 -*-
# main.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
# License: MIT - see the LICENSE file for the full text.

'''
This is the main file for the authnzerver, a simple authorization and
authentication server backed by SQLite and Tornado for use with the lcc-server.

'''

#############
## LOGGING ##
#############

import logging

# setup a logger
LOGMOD = __name__


#############
## IMPORTS ##
#############

import os
import os.path
import socket
import sys
import signal
import time
from functools import partial

# setup signal trapping on SIGINT
def _recv_sigint(signum, stack):
    '''
    handler function to receive and process a SIGINT

    '''
    raise KeyboardInterrupt


#####################
## TORNADO IMPORTS ##
#####################

# experimental, probably will remove at some point
try:
    import asyncio
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    IOLOOP_SPEC = 'uvloop'
except Exception as e:
    HAVE_UVLOOP = False
    IOLOOP_SPEC = 'asyncio'

import tornado.ioloop
import tornado.httpserver
import tornado.web
import tornado.options
from tornado.options import define, options
import multiprocessing as mp

###############################
### APPLICATION SETUP BELOW ###
###############################

modpath = os.path.abspath(os.path.dirname(__file__))

# define our commandline options

# the port to serve on
define('port',
       default=12690,
       help='Run on the given port.',
       type=int)

# the address to listen on
define('serve',
       default='127.0.0.1',
       help='Bind to given address and serve content.',
       type=str)

# whether to run in debugmode or not
define('debugmode',
       default=0,
       help='start up in debug mode if set to 1.',
       type=int)

# number of background threads in the pool executor
define('backgroundworkers',
       default=4,
       help=('number of background workers to use '),
       type=int)

# basedir is the directory at the root where this server stores its auth DB and
# looks for secret keys.
define('basedir',
       default=os.getcwd(),
       help=('The base directory containing secret files and the auth DB.'),
       type=str)

# the path to the authentication DB
define('authdb',
       default=None,
       help=('An SQLAlchemy database URL to override the use of '
             'the local authentication DB. '
             'This should be in the form discussed at: '
             'https://docs.sqlalchemy.org/en/latest'
             '/core/engines.html#database-urls'),
       type=str)

# the path to the cache directory used to enforce API limits
define('cachedir',
       default='/tmp/vizinspect-cache',
       help=('Path to the cache directory used by the authnzerver.'),
       type=str)


# the environment variable to get FERNETSECRET from.
define('secretenv',
       default='FERNETSECRET',
       help=('The environment variable used to get the secret key.'),
       type=str)

# the path to the secret file to get FERNETSECRET from.
define('secretfile',
       default='.server.secret-fernet',
       help=('Path to the file containing the secret key. '
             'This is relative to the path given in the basedir option.'),
       type=str)

define('sessionexpiry',
       default=30,
       help=('This sets the session-expiry time in days.'),
       type=int)


#######################
## UTILITY FUNCTIONS ##
#######################

def setup_auth_worker(authdb_path,
                      fernet_secret):
    '''This stores secrets and the auth DB path in the worker loop's context.

    The worker will then open the DB and set up its Fernet instance by itself.

    '''
    # unregister interrupt signals so they don't get to the worker
    # and the executor can kill them cleanly (hopefully)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    currproc = mp.current_process()
    currproc.auth_db_path = authdb_path
    currproc.fernet_secret = fernet_secret



def close_authentication_database():

    '''This is used to close the authentication database when the worker loop
    exits.

    '''

    currproc = mp.current_process()
    if getattr(currproc, 'table_meta', None):
        del currproc.table_meta

    if getattr(currproc, 'connection', None):
        currproc.connection.close()
        del currproc.connection

    if getattr(currproc, 'engine', None):
        currproc.engine.dispose()
        del currproc.engine

    print('Shutting down database engine in process: %s' % currproc.name,
          file=sys.stdout)


###########################################
## AUTO-GENERATION OF SECRETS AND AUTHDB ##
###########################################

def autogen_secrets_authdb(basedir, logger):
    '''
    This automatically generates a secrets file and auth DB.

    Run only once on the first start of an authnzerver.

    '''

    import getpass
    from .authdb import create_sqlite_auth_db, initial_authdb_inserts
    from cryptography.fernet import Fernet

    # create our authentication database if it doesn't exist
    authdb_path = os.path.join(basedir, '.authdb.sqlite')

    logger.warning('No existing authentication DB found, making a new one...')

    # generate the initial DB
    create_sqlite_auth_db(authdb_path, echo=False, returnconn=False)

    # ask the user for their email address and password the default
    # email address will be used for the superuser if the email address
    # is None, we'll use the user's UNIX ID@localhost if the password is
    # None, a random one will be generated

    try:
        userid = '%s@localhost' % getpass.getuser()
    except Exception as e:
        userid = 'serveradmin@localhost'

    inp_userid = input(
        '\nAdmin email address [default: %s]: ' %
        userid
    )
    if inp_userid and len(inp_userid.strip()) > 0:
        userid = inp_userid

    inp_username = input(
        'Admin user name [default: %s]: ' %
        userid.split('@')[0]
    )
    if inp_username and len(inp_username.strip()) > 0:
        username = inp_username
    else:
        username = userid.split('@')[0]

    inp_pass = getpass.getpass(
        'Admin password [default: randomly generated]: '
    )
    if inp_pass and len(inp_pass.strip()) > 0:
        password = inp_pass
    else:
        password = None

    # generate the admin users and initial DB info
    u, n, p = initial_authdb_inserts('sqlite:///%s' % authdb_path,
                                     superuser_email=userid,
                                     superuser_username=username,
                                     superuser_pass=password)

    creds = os.path.join(basedir,
                         '.server.admin-credentials')
    with open(creds,'w') as outfd:
        outfd.write('%s %s %s\n' % (u,n,p))
        os.chmod(creds, 0o100400)

    if p:
        logger.warning('Generated random admin password, written to: %s\n' %
                       creds)

    # finally, we'll generate the server secrets now so we don't have to deal
    # with them later
    logger.info('Generating server secret tokens...')
    fernet_secret = Fernet.generate_key()
    fernet_secret_file = os.path.join(basedir,'.server.secret-fernet')

    with open(fernet_secret_file,'wb') as outfd:
        outfd.write(fernet_secret)
    os.chmod(fernet_secret_file, 0o100400)

    session_secret = Fernet.generate_key()
    session_secret_file = os.path.join(basedir,'.server.secret-session')

    with open(session_secret_file,'wb') as outfd:
        outfd.write(session_secret)
    os.chmod(session_secret_file, 0o100400)

    return authdb_path, creds, fernet_secret_file, session_secret_file



##########
## MAIN ##
##########

def main():

    # parse the command line
    tornado.options.parse_command_line()

    DEBUG = True if options.debugmode == 1 else False

    # get a logger
    LOGGER = logging.getLogger(__name__)
    if DEBUG:
        LOGGER.setLevel(logging.DEBUG)
    else:
        LOGGER.setLevel(logging.INFO)

    ###################
    ## LOCAL IMPORTS ##
    ###################

    from ..utils import ProcExecutor

    ##############
    ## HANDLERS ##
    ##############

    from .handlers import AuthHandler, EchoHandler
    from . import authdb
    from . import cache
    from . import actions


    ###################
    ## SET UP CONFIG ##
    ###################

    MAXWORKERS = options.backgroundworkers

    # use the local sqlite DB as the default auth DB
    AUTHDB_SQLITE = os.path.join(options.basedir, '.authdb.sqlite')

    # search for the Fernet secret in either the environment variable
    # or the secret file path
    try:
        FERNETSECRET = authdb.get_secret_token(options.secretenv,
                                               os.path.join(
                                                   options.basedir,
                                                   options.secretfile
                                               ),LOGGER)

    except Exception as e:

        if ( (not os.path.exists(AUTHDB_SQLITE)) or
             (not os.path.exists(options.authdb.replace('sqlite:///',''))) ):

            authdb_p, creds, fernet_file, session_file = autogen_secrets_authdb(
                options.basedir,
                LOGGER
            )

            FERNETSECRET = authdb.get_secret_token(options.secretenv,
                                                   os.path.join(
                                                       options.basedir,
                                                       options.secretfile
                                                   ),LOGGER)

        else:
            raise IOError("Auth DB exists, "
                          "but no secret key was provided. "
                          "Use the %s environment variable to pass "
                          "this in." % options.envsecret)


    # pass the DSN to the SQLAlchemy engine
    if os.path.exists(AUTHDB_SQLITE):
        AUTHDB_PATH = 'sqlite:///%s' % os.path.abspath(AUTHDB_SQLITE)
    elif options.authdb:
        # if the local authdb doesn't exist, we'll use the DSN provided by the
        # user
        AUTHDB_PATH = options.authdb
    else:
        raise ConnectionError(
            "No auth DB connection available. "
            "The local auth DB is missing or "
            "no SQLAlchemy database URL was provided to override it"
        )
    #
    # this is the background executor we'll pass over to the handler
    #
    executor = ProcExecutor(max_workers=MAXWORKERS,
                            initializer=setup_auth_worker,
                            initargs=(AUTHDB_PATH,
                                      FERNETSECRET),
                            finalizer=close_authentication_database)

    # we only have one actual endpoint, the other one is for testing
    handlers = [
        (r'/', AuthHandler,
         {'authdb':AUTHDB_PATH,
          'fernet_secret':FERNETSECRET,
          'executor':executor}),
    ]

    if DEBUG:
        # put in the echo handler for debugging
        handlers.append(
            (r'/echo', EchoHandler,
             {'authdb':AUTHDB_PATH,
              'fernet_secret':FERNETSECRET,
              'executor':executor})
        )

    ########################
    ## APPLICATION SET UP ##
    ########################

    app = tornado.web.Application(
        debug=DEBUG,
        autoreload=False,  # this sometimes breaks Executors so disable it
    )

    # try to guard against the DNS rebinding attack
    # http://www.tornadoweb.org/en/stable/guide/security.html#dns-rebinding
    app.add_handlers(r'(localhost|127\.0\.0\.1)',
                     handlers)

    # start up the HTTP server and our application
    http_server = tornado.httpserver.HTTPServer(app)


    ######################################################
    ## CLEAR THE CACHE AND REAP OLD SESSIONS ON STARTUP ##
    ######################################################

    removed_items = cache.cache_flush(
        cache_dirname=options.cachedir
    )
    LOGGER.info('removed %s stale items from authdb cache' % removed_items)

    session_killer = partial(actions.auth_kill_old_sessions,
                             session_expiry_days=options.sessionexpiry,
                             override_authdb_path=AUTHDB_PATH)

    # run once at start up
    session_killer()

    ######################
    ## start the server ##
    ######################

    # register the signal callbacks
    signal.signal(signal.SIGINT, _recv_sigint)
    signal.signal(signal.SIGTERM, _recv_sigint)

    # make sure the port we're going to listen on is ok
    # inspired by how Jupyter notebook does this
    portok = False
    serverport = options.port
    maxtries = 10
    thistry = 0
    while not portok and thistry < maxtries:
        try:
            http_server.listen(serverport, options.serve)
            portok = True
        except socket.error as e:
            LOGGER.warning('%s:%s is already in use, trying port %s' %
                           (options.serve, serverport, serverport + 1))
            serverport = serverport + 1

    if not portok:
        LOGGER.error('Could not find a free port after %s tries, giving up' %
                     maxtries)
        sys.exit(1)

    LOGGER.info('Started authnzerver. listening on http://%s:%s' %
                (options.serve, serverport))
    LOGGER.info('Background worker processes: %s. IOLoop in use: %s' %
                (MAXWORKERS, IOLOOP_SPEC))
    LOGGER.info('Base directory is: %s' % os.path.abspath(options.basedir))


    # start the IOLoop and begin serving requests
    try:

        loop = tornado.ioloop.IOLoop.current()

        # add our periodic callback for the session-killer
        # runs daily
        periodic_session_kill = tornado.ioloop.PeriodicCallback(
            session_killer,
            86400000.0,
            jitter=0.1,
        )
        periodic_session_kill.start()

        # start the IOLoop
        loop.start()

    except KeyboardInterrupt:

        LOGGER.info('Received Ctrl-C: shutting down...')

        # close down the processpool
        executor.shutdown()
        time.sleep(2)

        tornado.ioloop.IOLoop.instance().stop()

        currproc = mp.current_process()
        if getattr(currproc, 'table_meta', None):
            del currproc.table_meta

        if getattr(currproc, 'connection', None):
            currproc.connection.close()
            del currproc.connection

        if getattr(currproc, 'engine', None):
            currproc.engine.dispose()
            del currproc.engine

        print('Shutting down database engine in process: %s' % currproc.name,
              file=sys.stdout)


# run the server
if __name__ == '__main__':
    main()
