#!/usr/bin/env python
# -*- coding: utf-8 -*-
# indexhandlers.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Apr 2018

'''
These are Tornado handlers for the AJAX actions.

'''

####################
## SYSTEM IMPORTS ##
####################

import logging
import os.path
import multiprocessing as mp
import json
from datetime import datetime
import pathlib

import numpy as np


# for generating encrypted token information
from cryptography.fernet import Fernet


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

#############
## LOGGING ##
#############

# get a logger
LOGGER = logging.getLogger(__name__)



#####################
## TORNADO IMPORTS ##
#####################

from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.escape import xhtml_escape
from tornado import web


###################
## LOCAL IMPORTS ##
###################

from .basehandler import BaseHandler

from ..backend import catalogs, images


######################
## WORKER FUNCTIONS ##
######################

def worker_make_plot(
        objectid,
        basedir,
        random_sample_percent=2.0,
        override_dbinfo=None,
        override_client=None,
        raiseonfail=False,
):
    '''
    This makes the main plot for a specific objectid.

    '''

    try:

        currproc = mp.current_process()

        if not override_client:
            bucket_client = currproc.bucket_client
        else:
            bucket_client = override_client

        if not override_dbinfo:
            conn, meta = currproc.connection, currproc.metadata
        else:
            conn, meta = override_dbinfo

        # get the plot if it exists
        objectplot = os.path.abspath(
            os.path.join(
                basedir,
                'viz-inspect-data',
                'plot-objectid-{objectid}.png'.format(objectid=objectid)
            )
        )

        if not os.path.exists(objectplot):

            made_plot = images.make_main_plot(
                objectid,
                (conn, meta),
                os.path.join(basedir, 'viz-inspect-data'),
                bucket_client=bucket_client,
                random_sample_percent=random_sample_percent,
            )
            objectplot = os.path.abspath(made_plot)

        return objectplot

    except Exception as e:
        LOGGER.exception("Could not get info for object: %s" % objectid)
        if raiseonfail:
            raise

        return None


def worker_get_object(
        objectid,
        basedir,
        userid,
        random_sample_percent=2.0,
        override_dbinfo=None,
        override_client=None,
        raiseonfail=False,
):
    '''
    This does the actual work of loading the object.

    Runs in an executor.

    - gets the object from the catalog CSV
    - gets the object's comments from the comments CSV
    - gets the object's plot. writes plot to the site_datadir
    - plot files have names like '{catalog_fname}-source-{index}-plot.png'
    - these are kept around until the server exits
    - if a plot file exists, we don't try to make a new one

    '''

    try:

        if not override_dbinfo:
            currproc = mp.current_process()
            conn, meta = currproc.connection, currproc.metadata
        else:
            conn, meta = override_dbinfo

        # this returns a list of objectinfo rows
        # one row per entry in the comments table for this object
        # we'll reform everything to a single dict suitable for JSON output
        # and turn the comments into a row of dicts per commenter
        objectinfo = catalogs.get_object(objectid,
                                         (conn, meta))

        comments = [
            {'comment_added_on':x['comment_added_on'],
             'comment_by_userid':x['comment_by_userid'],
             'comment_by_username':x['comment_by_username'],
             'comment_userset_flags':x['comment_userset_flags'],
             'comment_text':x['comment_text']} for x in objectinfo
            if x['comment_added_on'] is not None
        ]

        # make a list of the reviewers' user IDs
        reviewer_userid = list(
            set([x['reviewer_userid'] for x in objectinfo
                 if x['reviewer_userid'] is not None])
        )

        comments = sorted(
            comments,
            key=lambda x: (
                x['comment_added_on'] if x['comment_added_on'] else ''
            ),
            reverse=True
        )

        # we return a single dict with all of the object info collapsed into it
        objectinfo_dict = objectinfo[0]
        del objectinfo_dict['comment_added_on']
        del objectinfo_dict['comment_by_userid']
        del objectinfo_dict['comment_by_username']
        del objectinfo_dict['comment_userset_flags']
        del objectinfo_dict['comment_text']
        objectinfo_dict['reviewer_userid'] = reviewer_userid

        objectinfo_dict['filepath'] = 'redacted'

        objectplot = worker_make_plot(
            objectid,
            basedir,
            random_sample_percent=random_sample_percent,
            override_dbinfo=override_dbinfo,
            override_client=override_client,
            raiseonfail=raiseonfail
        )

        # set the readonly flag
        if (len(objectinfo_dict['reviewer_userid']) > 0 and
            userid in objectinfo_dict['reviewer_userid']):
            readonly = False
        elif (len(objectinfo_dict['reviewer_userid']) > 0 and
              userid not in objectinfo_dict['reviewer_userid']):
            readonly = True
        elif (len(objectinfo_dict['reviewer_userid']) == 0):
            readonly = False
        else:
            readonly = True

        # this is the dict we return
        retdict = {
            'info': objectinfo_dict,
            'plot':os.path.basename(objectplot),
            'comments':comments,
            'readonly':readonly
        }

        # touch the plot file so we know it was recently accessed and the cache
        # won't evict it because it's accessed often
        pathlib.Path(objectplot).touch()

        return retdict

    except Exception as e:
        LOGGER.exception("Could not get info for object: %s" % objectid)
        if raiseonfail:
            raise

        return None


def worker_get_objects(
        review_status='all',
        userid=None,
        start_keyid=1,
        max_objects=100,
        getinfo='objectids',
        override_dbinfo=None,
        raiseonfail=False,
):
    '''
    This returns the full object list.

    '''

    try:

        if not override_dbinfo:
            currproc = mp.current_process()
            conn, meta = currproc.connection, currproc.metadata
        else:
            conn, meta = override_dbinfo

        # figure out the page slices by looking up the object count
        list_count = catalogs.get_object_count(
            (conn, meta),
            review_status=review_status,
            userid=userid,
        )
        if (list_count % max_objects):
            n_pages = int(list_count/max_objects) + 1
        else:
            n_pages = int(list_count/max_objects)

        # this returns a list of tuples (keyid, objectid)
        objectlist, ret_start_keyid, ret_end_keyid = catalogs.get_objects(
            (conn, meta),
            review_status=review_status,
            userid=userid,
            start_keyid=start_keyid,
            max_objects=max_objects,
            getinfo=getinfo,
            fast_fetch=True
        )

        # reform to a single list
        returned_objectlist = [x[1] for x in objectlist]

        # this is the dict we return
        retdict = {
            'objectlist': list(set(returned_objectlist)),
            'start_keyid':ret_start_keyid,
            'end_keyid':ret_end_keyid,
            'object_count':list_count,
            'rows_per_page':max_objects,
            'n_pages':n_pages,
        }

        return retdict

    except Exception as e:
        LOGGER.exception("Could not get object list.")
        if raiseonfail:
            raise

        return None



def worker_insert_object_comments(
        userid,
        username,
        comments,
        override_dbinfo=None,
        raiseonfail=False,
):
    '''
    This inserts object comments.

    '''

    try:

        if not override_dbinfo:
            currproc = mp.current_process()
            conn, meta = currproc.connection, currproc.metadata
        else:
            conn, meta = override_dbinfo

        # this returns a list of dicts {'objectid': <objectid>}
        updated = catalogs.insert_object_comments(
            userid,
            comments,
            (conn, meta),
            username=username,
        )

        # this is the dict we return
        retdict = {
            'updated': updated == 1,
        }

        return retdict

    except Exception as e:
        LOGGER.exception("Could not insert the comments into the DB.")
        if raiseonfail:
            raise
        return None



def worker_update_object_flags(
        objectid,
        flags,
        override_dbinfo=None,
        raiseonfail=False,
):
    '''
    This updates the global object flags.

    '''



def worker_export_catalog(
        basedir,
        outdir='viz-inspect-data',
        override_dbinfo=None,
        raiseonfail=False,
):
    '''This exports the catalog from the DB to the output dir.

    By default the file is written to the viz-inspect-data dir under the
    basedir. This allows the server to serve it back to the client if they want
    to download it after exporting it.

    '''


def worker_list_review_assignments(
        list_type='unassigned',
        user_id=None,
        start_keyid=1,
        max_objects=500,
        override_dbinfo=None,
        raiseonfail=False,
):
    '''
    This lists review assignments.

    '''

    try:

        if not override_dbinfo:
            currproc = mp.current_process()
            conn, meta = currproc.connection, currproc.metadata
        else:
            conn, meta = override_dbinfo

        #
        # parse into dicts
        #

        if list_type == 'unassigned':

            # figure out the page slices by looking up the object count
            list_count = catalogs.get_object_count(
                (conn, meta),
                review_status='unassigned-all',
            )
            if (list_count % max_objects):
                list_n_pages = int(list_count/max_objects) + 1
            else:
                list_n_pages = int(list_count/max_objects)

            (list_objects,
             list_start_keyid,
             list_end_keyid) = catalogs.get_objects(
                 (conn, meta),
                 review_status='unassigned-all',
                 userid=None,
                 start_keyid=start_keyid,
                 max_objects=max_objects,
                 getinfo='objectids',
                 fast_fetch=True
            )

            final_objects = list(set([x[1] for x in list_objects]))

            # this is the dict we return
            retdict = {
                'rows_per_page':max_objects,
                'object_list': final_objects,
                'start_keyid':list_start_keyid,
                'end_keyid':list_end_keyid,
                'object_count':list_count,
                'n_pages':list_n_pages,
            }

            return retdict


        elif list_type == 'assigned' and user_id is not None:

            # figure out the page slices by looking up the object count
            list_count = catalogs.get_object_count(
                (conn, meta),
                review_status='assigned-self',
                userid=user_id
            )
            if (list_count % max_objects):
                list_n_pages = int(list_count/max_objects) + 1
            else:
                list_n_pages = int(list_count/max_objects)

            (list_objects,
             list_start_keyid,
             list_end_keyid) = catalogs.get_objects(
                 (conn, meta),
                 review_status='assigned-self',
                 userid=user_id,
                 start_keyid=start_keyid,
                 max_objects=max_objects,
                 getinfo='review-assignments',
                 fast_fetch=True
            )

            # we're only interested in the assigned object lists
            final_objects = sorted(list(set([x[1] for x in list_objects])))

            # this is the dict we return
            retdict = {
                'rows_per_page':max_objects,
                'object_list': final_objects,
                'start_keyid':list_start_keyid,
                'end_keyid':list_end_keyid,
                'object_count':list_count,
                'n_pages':list_n_pages,
            }

            return retdict

    except Exception as e:
        LOGGER.exception("Could not get review assignments.")
        if raiseonfail:
            raise

        return None



def worker_assign_reviewer(
        userid,
        assignment_list,
        do_unassign=False,
        override_dbinfo=None,
        raiseonfail=False,
):
    '''
    This assigns objects to a reviewer.

    '''

    try:

        if not override_dbinfo:
            currproc = mp.current_process()
            conn, meta = currproc.connection, currproc.metadata
        else:
            conn, meta = override_dbinfo

        updated_assignments = catalogs.update_review_assignments(
            assignment_list,
            userid,
            (conn, meta),
            do_unassign=do_unassign,
        )

        return updated_assignments

    except Exception as e:

        LOGGER.exception("Could not assign objects for review.")
        if raiseonfail:
            raise

        return False


#####################
## OBJECT HANDLERS ##
#####################

class ObjectListHandler(BaseHandler):
    '''
    This handles the /api/list-objects endpoint.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   siteinfo,
                   authnzerver,
                   session_expiry,
                   fernetkey,
                   ratelimit,
                   cachedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.ferneter = Fernet(fernetkey)
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.ratelimit = ratelimit
        self.cachedir = cachedir


    @gen.coroutine
    def get(self):
        '''This handles GET requests to the /api/list-objects endpoint.

        Parameters
        ----------

        review_status : str, optional, default = 'all'
            Sets the type of list retrieval:

            - 'all' -> all objects
            - 'reviewed-all' -> all objects that have been reviewed
            - 'unreviewed-all' -> all objects that have not been reviewed
            - 'reviewed-self' -> objects reviewed by this user
            - 'reviewed-other' -> objects reviewed by other users
            - 'assigned-self' -> objects assigned to this user
            - 'assigned-reviewed' -> objects assigned to self and reviewed
            - 'assigned-unreviewed' -> objects assigned to self but unreviewed
            - 'assigned-all' -> all objects assigned to some reviewer
            - 'unassigned-all' -> all objects not assigned to any reviewers

            For -self retrieval types, we'll get the userid out of the session
            dict.

        page : int, optional, default = 0
           The page number to retrieve.

        '''

        # check if we're actually logged in
        if not self.current_user:
            retdict = {'status':'failed',
                       'message':'You must be logged in to view objects.',
                       'result': None}
            self.set_status(401)
            self.write(retdict)
            raise web.Finish()

        # if the current user is anonymous or locked, ignore their request
        if self.current_user and self.current_user['user_role'] in ('anonymous',
                                                                    'locked'):
            retdict = {'status':'failed',
                       'message':'You must be logged in to view objects.',
                       'result': None}
            self.set_status(401)
            self.write(retdict)
            raise web.Finish()

        # otherwise, go ahead and process the request
        try:

            # parse the args
            review_status = xhtml_escape(
                self.get_argument('review_status','all')
            )

            if review_status not in ('all',
                                     'assigned-self',
                                     'assigned-reviewed',
                                     'assigned-unreviewed',
                                     'reviewed-all',
                                     'unreviewed-all',
                                     'reviewed-self',
                                     'reviewed-other'):
                raise ValueError("Unknown review status requested: '%s'" %
                                 review_status)

            start_keyid = int(
                xhtml_escape(self.get_argument('start_keyid', '1'))
            )
            max_objects = int(
                xhtml_escape(self.get_argument('max_objects',
                                               self.siteinfo['rows_per_page']))
            )

            objectlist_info = yield self.executor.submit(
                worker_get_objects,
                review_status=review_status,
                userid=self.current_user['user_id'],
                start_keyid=start_keyid,
                max_objects=max_objects,
                getinfo='objectids',
            )

            if objectlist_info is not None:

                retdict = {'status':'ok',
                           'message':'objectlist OK',
                           'result':objectlist_info}

            else:

                retdict = {'status':'failed',
                           'message':"Unable to retrieve object list.",
                           'result':None}
                self.set_status(404)

            self.write(retdict)
            self.finish()

        except Exception as e:

            LOGGER.exception('Failed to retrieve the object list.')
            self.set_status(400)
            retdict = {'status':'failed',
                       'message':'Invalid request for object list.',
                       'result':None}
            self.write(retdict)
            self.finish()



class LoadObjectHandler(BaseHandler):
    '''This handles the /api/load-object endpoint.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   siteinfo,
                   authnzerver,
                   session_expiry,
                   fernetkey,
                   ratelimit,
                   cachedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.ferneter = Fernet(fernetkey)
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.ratelimit = ratelimit
        self.cachedir = cachedir


    @gen.coroutine
    def get(self, objectid):
        '''This handles GET requests to the /api/load-object/<index> endpoint.

        Gets catalog and comment info, plots the object if not already plotted,
        and then returns JSON with everything.

        '''

        # check if we're actually logged in
        if not self.current_user:
            retdict = {'status':'failed',
                       'message':'You must be logged in to view objects.',
                       'result': None}
            self.set_status(401)
            self.write(retdict)
            raise web.Finish()

        # if the current user is anonymous or locked, ignore their request
        if self.current_user and self.current_user['user_role'] in ('anonymous',
                                                                    'locked'):
            retdict = {'status':'failed',
                       'message':'You must be logged in to view objects.',
                       'result': None}
            self.set_status(401)
            self.write(retdict)
            raise web.Finish()

        # otherwise, go ahead and process the request
        try:

            objindex = int(xhtml_escape(objectid))
            if objindex < 0:
                objindex = 0

            # if there's an argument saying to make plots of neighbors
            neighborhood = self.get_argument('neighborhood', None)
            if neighborhood:
                neighborhood = json.loads(neighborhood)

            # get the object information
            objectinfo = yield self.executor.submit(
                worker_get_object,
                objindex,
                self.basedir,
                self.current_user['user_id'],
                random_sample_percent=self.siteinfo['random_sample_percent']
            )

            if objectinfo is not None:

                retdict = {'status':'ok',
                           'message':'object found OK',
                           'result':objectinfo}

            else:

                retdict = {'status':'failed',
                           'message':"Object with specified ID not found.",
                           'result':None}
                self.set_status(404)

            self.write(retdict)
            self.finish()

            # when we're done with this object, see if we need to plot neighbors
            if neighborhood is not None:

                # we'll only make plots for up to 7 other objects
                for neighbor in neighborhood[:7]:

                    yield self.executor.submit(
                        worker_make_plot,
                        neighbor,
                        self.basedir,
                        random_sample_percent=(
                            self.siteinfo['random_sample_percent']
                        )
                    )

                    LOGGER.info("Background plot for %s done.")


        except Exception as e:

            LOGGER.exception('failed to get requested object ID: %r' % objectid)
            self.set_status(400)
            retdict = {'status':'failed',
                       'message':'Invalid request for object ID',
                       'result':None}
            self.write(retdict)
            self.finish()



class SaveObjectHandler(BaseHandler):
    '''This handles the /api/save-object/<objectid> endpoint.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   siteinfo,
                   authnzerver,
                   session_expiry,
                   fernetkey,
                   ratelimit,
                   cachedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.ferneter = Fernet(fernetkey)
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.ratelimit = ratelimit
        self.cachedir = cachedir


    @gen.coroutine
    def post(self, objectid):
        '''This handles POST requests to /api/save-object/<objectid>.

        This saves the current object.

        '''

        # check if we're actually logged in
        if not self.current_user:
            retdict = {'status':'failed',
                       'message':'You must be logged in to view objects.',
                       'result': None}
            self.set_status(401)
            self.write(retdict)
            raise web.Finish()

        # if the current user is anonymous or locked, ignore their request
        if self.current_user and self.current_user['user_role'] in ('anonymous',
                                                                    'locked'):
            retdict = {'status':'failed',
                       'message':'You must be logged in to view objects.',
                       'result': None}
            self.set_status(401)
            self.write(retdict)
            raise web.Finish()

        # check the POST request for validity
        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise web.Finish()


        try:

            objectid = int(xhtml_escape(objectid))
            comment_text = self.get_argument('comment_text',None)
            user_flags = self.get_argument('user_flags',None)
            userid = self.current_user['user_id']
            username = self.current_user['full_name']

            if comment_text is not None and len(comment_text.strip()) == 0:
                comment_text = ''

            if comment_text is not None or user_flags is not None:

                # check if the user is allowed to comment on this object
                objectinfo = yield self.executor.submit(
                    worker_get_object,
                    objectid,
                    self.basedir,
                    userid,
                )

                # if this object actually exists and is writable, we can do
                # stuff on it
                if objectinfo is not None and not objectinfo['readonly']:

                    commentdict = {'objectid':objectid,
                                   'comment':comment_text,
                                   'user_flags':json.loads(user_flags)}

                    updated = yield self.executor.submit(
                        worker_insert_object_comments,
                        userid,
                        username,
                        commentdict,
                    )

                    if updated is not None:

                        retdict = {'status':'ok',
                                   'message':'object updated OK',
                                   'result':updated}
                        self.write(retdict)
                        self.finish()

                    else:

                        retdict = {
                            'status':'failed',
                            'message':(
                                "Object with specified ID "
                                "could not be updated."
                            ),
                            'result':None
                        }
                        self.write(retdict)
                        self.finish()

                else:

                    retdict = {'status':'failed',
                               'message':(
                                   "Object not found, or is not in your "
                                   "list of objects to review. "
                                   "Your comments were not saved."
                               ),
                               'result':None}
                    self.write(retdict)
                    raise web.Finish()

            # if no comment text was supplied, do nothing
            else:

                retdict = {
                    'status':'ok',
                    'message':'No comments supplied. Object is unchanged.',
                    'result': None
                }
                self.write(retdict)
                self.finish()


        except Exception as e:

            LOGGER.exception('failed to save changes for object ID: %r' %
                             objectid)
            self.set_status(400)
            retdict = {'status':'failed',
                       'message':'Invalid save request for object ID',
                       'result':None}
            self.write(retdict)
            self.finish()



class ReviewAssignmentHandler(BaseHandler):
    '''
    This handles /api/review-assign.

    '''

    def initialize(self,
                   fernetkey,
                   executor,
                   authnzerver,
                   basedir,
                   session_expiry,
                   siteinfo,
                   ratelimit,
                   cachedir):
        '''
        This just sets up some stuff.

        '''

        self.authnzerver = authnzerver
        self.fernetkey = fernetkey
        self.ferneter = Fernet(fernetkey)
        self.executor = executor
        self.session_expiry = session_expiry
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.siteinfo = siteinfo
        self.ratelimit = ratelimit
        self.cachedir = cachedir
        self.basedir = basedir

        # initialize this to None
        # we'll set this later in self.prepare()
        self.current_user = None

        # apikey verification info
        self.apikey_verified = False
        self.apikey_info = None


    @gen.coroutine
    def get(self):
        '''
        This gets the lists of assigned or unassigned objects for review.

        '''

        if not self.current_user:
            self.redirect('/users/login')

        current_user = self.current_user

        # only allow in superuser roles
        if current_user and current_user['user_role'] == 'superuser':

            try:

                # ask the authnzerver for a user list
                reqtype = 'user-list'
                reqbody = {'user_id': None}

                ok, resp, msgs = yield self.authnzerver_request(
                    reqtype, reqbody
                )

                if not ok:

                    LOGGER.error('no user list returned from authnzerver')
                    user_list = []

                else:
                    user_list = [
                        x['user_id'] for x in resp['user_info'] if
                        x['user_id'] not in (2,3)
                    ]

                list_type = xhtml_escape(
                    self.get_argument('list_type','unassigned')
                )
                start_keyid = int(
                    xhtml_escape(
                        self.get_argument('start_keyid','1')
                    )
                )
                get_user_id = self.get_argument('user_id', 'all')

                if get_user_id.strip() != 'all':
                    get_user_id = int(xhtml_escape(get_user_id.strip()))
                    if get_user_id not in user_list:
                        get_user_id = None

                else:
                    get_user_id = None

                # getting unassigned objects
                if list_type == 'unassigned':

                    # get the review assignments
                    reviewlist_info = yield self.executor.submit(
                        worker_list_review_assignments,
                        list_type='unassigned',
                        start_keyid=start_keyid,
                        max_objects=500,
                    )

                # for assigned objects, we'll do it per user
                elif list_type == 'assigned' and get_user_id is None:

                    reviewlist_info = {}

                    for userid in user_list:

                        reviewlist_info[userid] = yield self.executor.submit(
                            worker_list_review_assignments,
                            list_type='assigned',
                            start_keyid=start_keyid,
                            max_objects=500,
                            user_id=userid,
                        )

                # for assigned objects and a single user
                elif list_type == 'assigned' and get_user_id is not None:

                    reviewlist_info = {}

                    reviewlist_info[get_user_id] = yield self.executor.submit(
                        worker_list_review_assignments,
                        list_type=list_type,
                        start_keyid=start_keyid,
                        max_objects=500,
                        user_id=get_user_id,
                    )

                if reviewlist_info is not None:

                    retdict = {'status':'ok',
                               'message':'objectlist OK',
                               'result':reviewlist_info}

                else:

                    retdict = {'status':'failed',
                               'message':"Unable to retrieve object list.",
                               'result':None}

                self.write(retdict)
                self.finish()

            except Exception as e:

                LOGGER.exception("Failed to list assignments")
                self.set_status(400)
                retdict = {
                    'status':'failed',
                    'result':None,
                    'message':("Sorry, could not list assignments.")
                }
                self.write(retdict)
                self.finish()

        # anything else is not allowed, turn them away
        else:
            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            self.finish()



    @gen.coroutine
    def post(self):
        '''This handles the POST to /api/review-assign.

        '''
        if not self.current_user:
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise web.Finish()

        # get the current user
        current_user = self.current_user

        # only allow in superuser roles
        if current_user and current_user['user_role'] == 'superuser':

            try:

                target_userid = int(
                    xhtml_escape(self.get_argument('userid',None))
                )
                target_objectlist = json.loads(
                    self.get_argument('assigned_objects',None)
                )
                target_objectlist = [int(x) for x in target_objectlist]

                do_unassign_flag = int(
                    xhtml_escape(self.get_argument('unassign_flag','0'))
                )

                # this is the normal assign mode
                if do_unassign_flag == 0:

                    # get the review assignments
                    assigned_ok = yield self.executor.submit(
                        worker_assign_reviewer,
                        target_userid,
                        target_objectlist,
                        do_unassign=False,
                    )

                    if assigned_ok:

                        retdict = {'status':'ok',
                                   'message':'objects assigned OK',
                                   'result':assigned_ok}

                    else:

                        retdict = {
                            'status':'failed',
                            'message':"Unable to assign objects to reviewer.",
                            'result':None
                        }

                    self.write(retdict)
                    self.finish()

                # otherwise, we're doing unassigning of objects for a user
                else:

                    # get the review assignments
                    unassigned_ok = yield self.executor.submit(
                        worker_assign_reviewer,
                        target_userid,
                        target_objectlist,
                        do_unassign=True,
                    )

                    if unassigned_ok:

                        retdict = {'status':'ok',
                                   'message':'objects unassigned OK',
                                   'result':unassigned_ok}

                    else:

                        retdict = {
                            'status':'failed',
                            'message':(
                                "Unable to unassign objects from reviewer."
                            ),
                            'result': None
                        }

                    self.write(retdict)
                    self.finish()

            except Exception as e:

                LOGGER.exception("Failed to understand request.")
                self.set_status(400)
                retdict = {
                    'status':'failed',
                    'result':None,
                    'message':("Unknown or invalid arguments provided.")
                }
                self.write(retdict)
                self.finish()

        else:

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            self.finish()
