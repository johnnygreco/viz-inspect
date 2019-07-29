#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''This is the main server module.

'''

#############
## LOGGING ##
#############

import logging


#############
## IMPORTS ##
#############

import os
import os.path
import signal
import time
import sys
import socket
import json
import multiprocessing as mp
from datetime import datetime
import subprocess
from functools import partial

import numpy as np


# setup signal trapping on SIGINT
def recv_sigint(signum, stack):
    '''
    handler function to receive and process a SIGINT

    '''
    raise KeyboardInterrupt


class FrontendEncoder(json.JSONEncoder):
    '''
    This handles encoding weird things.

    '''

    def default(self, obj):

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode()
        elif isinstance(obj, complex):
            return (obj.real, obj.imag)
        elif (isinstance(obj, (float, np.float64, np.float_)) and
              not np.isfinite(obj)):
            return None
        elif isinstance(obj, (np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        else:
            return json.JSONEncoder.default(self, obj)


# this replaces the default encoder and makes it so Tornado will do the right
# thing when it converts dicts to JSON when a
# tornado.web.RequestHandler.write(dict) is called.
json._default_encoder = FrontendEncoder()


#####################
## TORNADO IMPORTS ##
#####################

# experimental, probably will remove at some point
try:
    import asyncio
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    IOLOOP_SPEC = 'uvloop'
except Exception:
    HAVE_UVLOOP = False
    IOLOOP_SPEC = 'asyncio'

import tornado.ioloop
import tornado.httpserver
import tornado.web
import tornado.options
from tornado.options import define, options


###############################
### APPLICATION SETUP BELOW ###
###############################

modpath = os.path.abspath(os.path.dirname(__file__))

# define our commandline options

# the port to serve on
# indexserver  will serve on 14005 by default
define('port',
       default=14005,
       help='Run on the given port.',
       type=int)

# the address to listen on
define('serve',
       default='0.0.0.0',
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

# the template path
define('templatepath',
       default=os.path.abspath(os.path.join(modpath,'templates')),
       help=('Sets the tornado template path.'),
       type=str)

# the assetpath
define('assetpath',
       default=os.path.abspath(os.path.join(modpath,'static')),
       help=('Sets the asset (server images, css, JS) path.'),
       type=str)

# basedir is the directory where the server will work.
define('basedir',
       default=os.getcwd(),
       help=('The base work directory of server.'),
       type=str)

## this tells the testserver about the backend authnzerver
define('authnzerver',
       default='http://127.0.0.1:12690',
       help=('This tells the server the address of '
             'the local authentication and authorization server.'),
       type=str)

## this tells the testserver about the default session expiry time in days
define('sessionexpiry',
       default=30,
       help=('This tells the server the session-expiry time in days.'),
       type=int)


###########################
## DATABASE AND CATALOGS ##
###########################

define('catalogcsv',
       default=None,
       help=("This tells the server to load the provided catalog into the DB."),
       type=str)

define('imagedir',
       default=None,
       help=("This tells the server where the HUGS images are."),
       type=str)

define('flagkeys',
       default='galaxy, candy, junk, tidal, outskirts, cirrus',
       help=("This tells the server what object flags to use for the catalog."),
       type=str)

define('firststart',
       default=False,
       help=("This tells the server to assume "
             "that we're starting over from scratch "
             "and to recreate all the DBs, etc.."),
       type=bool)


###########
## UTILS ##
###########

def setup_worker(siteinfo):
    '''This sets up the workers to ignore the INT signal, which is handled by
    the main process.

    Sets up the backend database instance. Also sets up the bucket client if
    required.

    '''

    from ..backend import database

    # unregister interrupt signals so they don't get to the worker
    # and the executor can kill them cleanly (hopefully)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # set up the database
    currproc = mp.current_process()

    # sets up the engine, connection, and metadata objects as process-local
    # variables
    currproc.engine, currproc.connection, currproc.metadata = (
        database.get_vizinspect_db(
            siteinfo['database_url'],
            database.VIZINSPECT
        )
    )

    if siteinfo['images_are_remote']:

        from vizinspect import bucketstorage

        currproc.bucket_client = bucketstorage.client(
            (siteinfo['access_token'], siteinfo['secret_key']),
            region=siteinfo['region'],
            endpoint=siteinfo['endpoint']
        )

    else:
        currproc.bucket_client = None


def close_database():

    '''This is used to close the database when the worker loop
    exits.

    '''

    currproc = mp.current_process()
    if getattr(currproc, 'metadata', None):
        del currproc.metadata

    if getattr(currproc, 'connection', None):
        currproc.connection.close()
        del currproc.connection

    if getattr(currproc, 'engine', None):
        currproc.engine.dispose()
        del currproc.engine

    print('Shutting down database engine in process: %s' % currproc.name,
          file=sys.stdout)


############
### MAIN ###
############

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

    ###########################
    ## DEFINING URL HANDLERS ##
    ###########################

    from ..authnzerver import authdb
    from . import auth_handlers as ah
    from . import indexhandlers as ih
    from . import actionhandlers as actions
    from . import admin_handlers as admin

    ###################
    ## SET UP CONFIG ##
    ###################

    def periodic_cleanup_worker(imagedir=None, retention_days=7):
        '''
        This is a periodic worker to remove older images from imagedir.

        '''

        cmd = (
            r"find {imagedir} -type f -name '*.png' "
            r"-mtime +{mtime} -exec rm -v '{{}}' \;"
        ).format(imagedir=imagedir,
                 mtime=retention_days)

        try:
            LOGGER.info("Deleting images older than %s days in %s." %
                        (retention_days, imagedir))
            proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
            ndeleted = len(proc.stdout.decode().split('\n'))
            LOGGER.warning('%s files older than %s days deleted.' %
                           (ndeleted, retention_days))
        except Exception:
            LOGGER.exception('Could not delete old files.')

    MAXWORKERS = options.backgroundworkers

    # various directories we need
    BASEDIR = os.path.abspath(options.basedir)
    TEMPLATEPATH = os.path.abspath(options.templatepath)
    ASSETPATH = os.path.abspath(options.assetpath)
    CURRENTDIR = os.path.abspath(os.getcwd())

    # get our secret keys
    SESSIONSECRET = authdb.get_secret_token(
        'SESSIONSECRET',
        os.path.join(
            BASEDIR,
            '.server.secret-session'
        ),
        LOGGER
    )
    FERNETSECRET = authdb.get_secret_token(
        'FERNETSECRET',
        os.path.join(
            BASEDIR,
            '.server.secret-fernet'
        ),
        LOGGER
    )

    # check if there's a first_start_done file to see if we need to copy over a
    # site-info.json and email-server.json file to the basedir. also copy over
    # the example bits
    first_start_done_file = os.path.join(options.basedir,
                                         '.first_start_done')
    first_start_done = os.path.exists(first_start_done_file)

    # on the first start, the server should ask for a catalog CSV and the flag
    # values
    if not first_start_done or options.firststart:

        import shutil

        # copy over the site-info.json file
        try:
            shutil.copy(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                     '..',
                                                     'data',
                                                     'site-info.json')),
                        os.path.abspath(options.basedir))
        except FileExistsError:
            LOGGER.warning("site-info.json already exists "
                           "in the basedir. Not overwriting.")

        # copy over the email-server.json file
        try:
            shutil.copy(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                     '..',
                                                     'data',
                                                     'email-server.json')),
                        os.path.abspath(options.basedir))
        except FileExistsError:
            LOGGER.warning("email-server.json already exists "
                           "in the basedir. Not overwriting.")

        # make a default data directory
        try:
            os.makedirs(os.path.join(options.basedir,'viz-inspect-data'))
        except FileExistsError:
            LOGGER.warning("The output plot PNG directory already "
                           "exists in the basedir. Not overwriting.")

        #
        # now, get site specific info
        #
        siteinfojson = os.path.join(BASEDIR, 'site-info.json')
        with open(siteinfojson,'r') as infd:
            SITEINFO = json.load(infd)

        # 0a. confirm the flags to be used for this project
        LOGGER.info('Please confirm the object flags '
                    'that will be used for this project.')
        flag_keys = input("Object flags [default: %s]: " %
                          options.flagkeys)
        if not flag_keys or len(flag_keys.strip()) == 0:
            set_flag_keys = options.flagkeys
        else:
            set_flag_keys = flag_keys
            SITEINFO['flag_keys'] = set_flag_keys

        # 0a. confirm the good flags
        LOGGER.info("Which object flags are associated with 'good' objects?")
        good_flag_keys = input("Good object flags [default: galaxy, candy]: ")
        if not good_flag_keys or len(flag_keys.strip()) == 0:
            set_good_flag_keys = 'galaxy, candy'
        else:
            set_good_flag_keys = good_flag_keys
            SITEINFO['good_flag_keys'] = set_good_flag_keys

        # 0b. confirm the bad flags
        LOGGER.info("Which object flags are associated with 'bad' objects?")
        bad_flag_keys = input("Bad object flags [default: cirrus, junk]: ")
        if not bad_flag_keys or len(flag_keys.strip()) == 0:
            set_bad_flag_keys = 'cirrus, junk'
        else:
            set_bad_flag_keys = bad_flag_keys
            SITEINFO['bad_flag_keys'] = set_bad_flag_keys

        # 0c. confirm how many good flags are needed for object completion
        LOGGER.info("How many votes for 'good' flags are required to mark an "
                    "object as complete?")
        max_good_votes = input("Maximum good flag votes [default: 2]: ")
        if not max_good_votes or len(flag_keys.strip()) == 0:
            set_max_good_votes = 2
        else:
            set_max_good_votes = int(max_good_votes)
            if set_max_good_votes <= 0:
                set_max_good_votes = 2
            SITEINFO['max_good_votes'] = set_max_good_votes

        # 0c. confirm how many bad flags are needed for object completion
        LOGGER.info("How many votes for 'bad' flags are required to mark an "
                    "object as complete?")
        max_bad_votes = input("Maximum bad flag votes [default: 2]: ")
        if not max_bad_votes or len(flag_keys.strip()) == 0:
            set_max_bad_votes = 2
        else:
            set_max_bad_votes = int(max_bad_votes)
            if set_max_bad_votes <= 0:
                set_max_bad_votes = 2
            SITEINFO['max_bad_votes'] = set_max_bad_votes

        # 1. check if the --catalogcsv arg is present
        if (options.catalogcsv is not None and
            os.path.exists(options.catalogcsv)):

            LOGGER.info(
                "Doing first time setup. "
                "Loading provided catalog: %s into DB at %s" %
                (options.catalogcsv,
                 SITEINFO['database_url'])
            )
            catalog_path = options.catalogcsv

        else:

            LOGGER.info("First time setup requires a catalog CSV to load.")
            catalog_path = input("Catalog CSV location: ")

        # 2. check if the --imagedir arg is present
        if (options.imagedir is not None and
            os.path.exists(options.imagedir)):

            LOGGER.info(
                "Using dir: %s as the location of the HUGS images." %
                (options.imagedir,)
            )
            image_dir = options.imagedir

        else:

            LOGGER.info(
                "First time setup requires an "
                "image directory to load HUGS images from. "
                "If your images are in a Digital Ocean Spaces bucket, "
                "use 'dos://<bucket-name>' here. "
                "If your images are on AWS S3, use 's3://<bucket-name>' here."
            )
            image_dir = input("HUGS image directory location: ")

        # 3. confirm the database_url in the site-info.json file
        LOGGER.info('Please confirm the database URL '
                    'used to connect to the PostgreSQL DB server.')
        database_url = input("Database URL [default: %s]: " %
                             SITEINFO['database_url'])
        if not database_url or len(database_url.strip()) == 0:
            set_database_url = SITEINFO['database_url']
        else:
            set_database_url = database_url
            SITEINFO['database_url'] = set_database_url

        # 4. if the image directory indicates it's dos:// or s3://, ask for
        # credentials for the service, the
        if image_dir.startswith('s3://') or image_dir.startswith('dos://'):

            LOGGER.info(
                "Image directory is '%s'. "
                "An access token and secret key pair is required." % image_dir
            )
            access_token = input("Access Token for '%s': " % image_dir)
            secret_key = input("Secret Key for '%s': " % image_dir)

            LOGGER.info("We also need a region and endpoint URL.")

            default_dos_region = 'sfo2'
            default_dos_endpoint = 'https://sfo2.digitaloceanspaces.com'
            default_s3_region = 'us-east-1'
            default_s3_endpoint = 'https://s3.amazonaws.com'

            if image_dir.startswith('dos://'):
                region = input(
                    "Bucket region [default: %s]: " % default_dos_region
                )
                if not region or len(region.strip()) == 0:
                    region = default_dos_region
                endpoint = input(
                    "Bucket endpoint [default: %s]: " % default_dos_endpoint
                )
                if not endpoint or len(endpoint.strip()) == 0:
                    endpoint = default_dos_endpoint

            elif image_dir.startswith('s3://'):
                region = input("Region [default: %s]: " % default_s3_region)
                if not region or len(region.strip()) == 0:
                    region = default_s3_region
                endpoint = input(
                    "Endpoint [default: %s]: " % default_s3_endpoint
                )
                if not endpoint or len(endpoint.strip()) == 0:
                    endpoint = default_s3_endpoint

            # update the site-info.json file with these values
            SITEINFO['access_token'] = access_token
            SITEINFO['secret_key'] = secret_key
            SITEINFO['region'] = region
            SITEINFO['endpoint'] = endpoint
            SITEINFO['images_are_remote'] = True

        else:

            SITEINFO['access_token'] = None
            SITEINFO['secret_key'] = None
            SITEINFO['region'] = None
            SITEINFO['endpoint'] = None
            SITEINFO['images_are_remote'] = False

        # ask for the length of time in days that downloaded images
        # and generated plots will be left around
        LOGGER.info("To save local disk space, "
                    "older generated plots and downloaded "
                    "remote images will be periodically deleted.")
        default_retention_days = 15
        retention_days = input(
            "How long should these be kept on disk? [in days, default: %s]: " %
            default_retention_days
        )
        if not retention_days or len(retention_days.strip()) == 0:
            retention_days = default_retention_days
        else:
            retention_days = int(retention_days)

        SITEINFO['retention_days'] = retention_days

        # ask for the sampling percentage of the object rows to use for the
        # plots
        LOGGER.info("To make the server more responsive, "
                    "only a certain percentage of objects in the "
                    "database will be used to make plots.")
        default_random_sample_percent = 2.0
        random_sample_percent = input(
            "Percentage of rows to randomly sample for plots "
            "[1.0-100.0, default: %.1f]: " %
            default_random_sample_percent
        )
        if not random_sample_percent or len(random_sample_percent.strip()) == 0:
            random_sample_percent = default_random_sample_percent
        else:
            random_sample_percent = float(random_sample_percent)

        SITEINFO['random_sample_percent'] = random_sample_percent

        # ask for the rows per page
        LOGGER.info("To make the server more responsive, "
                    "object lists will be paginated.")
        default_rows_per_page = 100
        rows_per_page = input(
            "Number of objects per page to use "
            "[integer, default: %i]: " %
            default_rows_per_page
        )
        if not rows_per_page or len(rows_per_page.strip()) == 0:
            rows_per_page = default_rows_per_page
        else:
            rows_per_page = float(rows_per_page)

        SITEINFO['rows_per_page'] = rows_per_page

        #
        # done with config
        #

        # update the site-info.json file
        with open(siteinfojson,'w') as outfd:
            json.dump(SITEINFO, outfd, indent=2)

        # make it readable/writeable by this user only
        os.chmod(siteinfojson, 0o100600)

        # now we have the catalog CSV and image dir
        # load the objects into the DB
        from ..backend import database, catalogs
        try:
            database.new_vizinspect_db(set_database_url,
                                       database.VIZINSPECT)
        except Exception:
            LOGGER.warning("The required tables already exist. "
                           "Will add this catalog to them.")

        LOGGER.info("Loading objects. Using provided flag keys: %s" %
                    options.flagkeys)

        # ask if existing objects should be overwritten
        overwrite_ask = input(
            "Should existing objects be overwritten? [Y/n]: "
        )
        if not overwrite_ask or len(overwrite_ask.strip()) == 0:
            overwrite = True
        elif overwrite_ask.strip().lower() == 'n':
            overwrite = False
        else:
            overwrite = True

        loaded = catalogs.load_catalog(
            catalog_path,
            image_dir,
            (set_database_url,
             database.VIZINSPECT),
            overwrite=overwrite,
            flags_to_use=[
                x.strip() for x in SITEINFO['flag_keys'].split(',')
            ]
        )

        if loaded:
            LOGGER.info("Objects loaded into catalog successfully.")

        #
        # end of first time setup
        #
        # set the first start done flag
        with open(first_start_done_file,'w') as outfd:
            outfd.write('server set up in this directory on %s UTC\n' %
                        datetime.utcnow().isoformat())

        LOGGER.info("First run setup for vizserver complete.")

    #
    # now, get site specific info
    #
    siteinfojson = os.path.join(BASEDIR, 'site-info.json')
    with open(siteinfojson,'r') as infd:
        SITEINFO = json.load(infd)

    # get the email info file if it exists
    if ('email_settings_file' in SITEINFO and
        os.path.exists(os.path.abspath(SITEINFO['email_settings_file']))):

        with open(SITEINFO['email_settings_file'],'r') as infd:
            email_settings = json.load(infd)

        if email_settings['email_server'] != "smtp.emailserver.org":
            SITEINFO.update(email_settings)

            LOGGER.info('Site info: email server to use: %s:%s.' %
                        (email_settings['email_server'],
                         email_settings['email_port']))
            LOGGER.info('Site info: email server sender to use: %s.' %
                        email_settings['email_sender'])

        else:
            LOGGER.warning('Site info: no email server is set up.')
            SITEINFO['email_server'] = None
    else:
        LOGGER.warning('Site info: no email server is set up.')
        SITEINFO['email_server'] = None

    # get the user login settings
    if SITEINFO['email_server'] is None:
        LOGGER.warning('Site info: '
                       'no email server set up, '
                       'user logins cannot be enabled.')
        SITEINFO['logins_allowed'] = False

    elif ('logins_allowed' in SITEINFO and
          SITEINFO['logins_allowed'] and
          SITEINFO['email_server'] is not None):
        LOGGER.info('Site info: user logins are allowed.')

    elif ('logins_allowed' in SITEINFO and (not SITEINFO['logins_allowed'])):
        LOGGER.warning('Site info: user logins are disabled.')

    else:
        SITEINFO['logins_allowed'] = False
        LOGGER.warning('Site info: '
                       'settings key "logins_allowed" not found, '
                       'disabling user logins.')

    # get the user signup and signin settings
    if SITEINFO['email_server'] is None:
        LOGGER.warning('Site info: '
                       'no email server set up, '
                       'user signups cannot be enabled.')
        SITEINFO['signups_allowed'] = False

    elif ('signups_allowed' in SITEINFO and
          SITEINFO['signups_allowed'] and
          SITEINFO['email_server'] is not None):
        LOGGER.info('Site info: user signups are allowed.')

    elif 'signups_allowed' in SITEINFO and not SITEINFO['signups_allowed']:
        LOGGER.warning('Site info: user signups are disabled.')

    else:
        SITEINFO['signups_allowed'] = False
        LOGGER.warning('Site info: '
                       'settings key "signups_allowed" not found, '
                       'disabling user signups.')

    #
    # authentication server options
    #
    AUTHNZERVER = options.authnzerver
    SESSION_EXPIRY = options.sessionexpiry

    #
    # rate limit options
    #
    RATELIMIT = SITEINFO['rate_limit_active']
    CACHEDIR = SITEINFO['cache_location']

    ###########################
    ## WORK AROUND APPLE BUG ##
    ###########################

    # here, we have to initialize networking in the main thread
    # before forking for MacOS. see:
    # https://bugs.python.org/issue30385#msg293958
    # if this doesn't work, Python will segfault.
    # the workaround noted in the report is to launch
    # lcc-server like so:
    # env no_proxy='*' indexserver
    if sys.platform == 'darwin':
        import requests
        requests.get('http://captive.apple.com/hotspot-detect.html')

    ####################################
    ## PERSISTENT BACKGROUND EXECUTOR ##
    ####################################

    #
    # this is the background executor we'll pass over to the handler
    #
    EXECUTOR = ProcExecutor(max_workers=MAXWORKERS,
                            initializer=setup_worker,
                            initargs=(SITEINFO,),
                            finalizer=close_database)

    ##################
    ## URL HANDLERS ##
    ##################

    HANDLERS = [

        #################
        ## BASIC STUFF ##
        #################

        # index page
        (r'/',
         ih.IndexHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        ###################################
        ## STATIC FILE DOWNLOAD HANDLERS ##
        ###################################

        # this handles static file downloads for collection info
        (r'/viz-inspect-data/(.*)',
         tornado.web.StaticFileHandler,
         {'path':SITEINFO['data_path']}),


        ##########################
        ## ACTUAL WORK HANDLERS ##
        ##########################

        (r'/api/list-objects',
         actions.ObjectListHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        (r'/api/load-object/(\d{1,10})',
         actions.LoadObjectHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        (r'/api/save-object/(\d{1,10})',
         actions.SaveObjectHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        ########################
        ## AUTH RELATED PAGES ##
        ########################

        # this is the login page
        (r'/users/login',
         ah.LoginHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the logout page
        (r'/users/logout',
         ah.LogoutHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the new user page
        (r'/users/new',
         ah.NewUserHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the verification page for verifying email addresses
        (r'/users/verify',
         ah.VerifyUserHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is step 1 page for forgotten passwords
        (r'/users/forgot-password-step1',
         ah.ForgotPassStep1Handler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the verification page for verifying email addresses
        (r'/users/forgot-password-step2',
         ah.ForgotPassStep2Handler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the password change page
        (r'/users/password-change',
         ah.ChangePassHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the user-prefs page
        (r'/users/home',
         ah.UserHomeHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the user-delete page
        (r'/users/delete',
         ah.DeleteUserHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        ####################
        ## ADMIN HANDLERS ##
        ####################

        # this is the admin index page
        (r'/admin',
         admin.AdminIndexHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'basedir':BASEDIR,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this handles email settings updates
        (r'/admin/email',
         admin.EmailSettingsHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'basedir':BASEDIR,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this handles user updates
        (r'/admin/users',
         admin.UserAdminHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'basedir':BASEDIR,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

    ]

    ########################
    ## APPLICATION SET UP ##
    ########################

    app = tornado.web.Application(
        static_path=ASSETPATH,
        handlers=HANDLERS,
        template_path=TEMPLATEPATH,
        static_url_prefix='/static/',
        compress_response=True,
        cookie_secret=SESSIONSECRET,
        xsrf_cookies=True,
        xsrf_cookie_kwargs={'samesite':'Lax'},
        debug=DEBUG,
    )

    # FIXME: consider using this instead of handlers=HANDLERS above.
    # http://www.tornadoweb.org/en/stable/guide/security.html#dns-rebinding
    # FIXME: how does this work for X-Real-Ip and X-Forwarded-Host?
    # if options.serve == '127.0.0.1':
    #     app.add_handlers(r'(localhost|127\.0\.0\.1)', HANDLERS)
    # else:
    #     fqdn = socket.getfqdn()
    #     ip = options.serve.replace('.','\.')
    #     app.add_handlers(r'({fqdn}|{ip})'.format(fqdn=fqdn,ip=ip), HANDLERS)

    # start up the HTTP server and our application. xheaders = True turns on
    # X-Forwarded-For support so we can see the remote IP in the logs
    http_server = tornado.httpserver.HTTPServer(app, xheaders=True)

    ######################
    ## start the server ##
    ######################

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
        except socket.error:
            LOGGER.warning('%s:%s is already in use, trying port %s' %
                           (options.serve, serverport, serverport + 1))
            serverport = serverport + 1

    if not portok:
        LOGGER.error('could not find a free port after %s tries, giving up' %
                     maxtries)
        sys.exit(1)

    LOGGER.info('Started vizserver. listening on http://%s:%s' %
                (options.serve, serverport))
    LOGGER.info('Background worker processes: %s, IOLoop in use: %s' %
                (MAXWORKERS, IOLOOP_SPEC))
    LOGGER.info('The current base directory is: %s' % os.path.abspath(BASEDIR))

    # register the signal callbacks
    signal.signal(signal.SIGINT,recv_sigint)
    signal.signal(signal.SIGTERM,recv_sigint)

    # start the IOLoop and begin serving requests
    try:

        loop = tornado.ioloop.IOLoop.current()

        periodic_clean = partial(
            periodic_cleanup_worker,
            imagedir=SITEINFO['data_path'],
            retention_days=SITEINFO['retention_days']
        )

        # run once at start
        periodic_clean()

        # add our periodic callback for the imagedir cleanup
        # runs every 24 hours
        periodic_imagedir_clean = tornado.ioloop.PeriodicCallback(
            periodic_clean,
            86400000.0,
            jitter=0.1,
        )
        periodic_imagedir_clean.start()

        # start the IOLoop
        loop.start()

    except KeyboardInterrupt:

        LOGGER.info('received Ctrl-C: shutting down...')
        loop.stop()
        # close down the processpool

    EXECUTOR.shutdown()
    time.sleep(2)


# run the server
if __name__ == '__main__':
    main()
