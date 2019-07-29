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
import multiprocessing as mp
import json
from datetime import datetime

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
from ..backend import catalogs


######################
## WORKER FUNCTIONS ##
######################

def worker_get_object(
        userid,
        objectid,
        basedir,
        override_dbinfo=None,
        override_client=None,
        raiseonfail=False,
):
    '''
    This does the actual work of loading the object.

    Runs in an executor.

    - gets the object from the catalog CSV
    - gets the object's comments from the comments CSV

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

        comments = sorted(
            comments,
            key=lambda x: (
                x['comment_added_on'] if x['comment_added_on'] else ''
            ),
            reverse=True
        )

        already_reviewed = (
            userid in (comment['comment_by_userid'] for comment in comments)
        )

        # we return a single dict with all of the object info collapsed into it
        objectinfo_dict = objectinfo[0]
        del objectinfo_dict['comment_added_on']
        del objectinfo_dict['comment_by_userid']
        del objectinfo_dict['comment_by_username']
        del objectinfo_dict['comment_userset_flags']
        del objectinfo_dict['comment_text']

        # this is the dict we return
        retdict = {
            'info': objectinfo_dict,
            'comments':comments,
            'review_status':objectinfo_dict['review_status'],
            'already_reviewed':already_reviewed
        }

        return retdict

    except Exception:
        LOGGER.exception("Could not get info for object: %s" % objectid)
        if raiseonfail:
            raise

        return None


def worker_get_objects(
        review_status='all',
        userid=None,
        start_keyid=1,
        end_keyid=None,
        max_objects=100,
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

        if review_status == 'all':
            check_review_status = 'all'
            userid_check = None
        elif review_status == 'incomplete':
            check_review_status = 'incomplete'
            userid_check = None
        elif review_status == 'complete-good':
            check_review_status = 'complete-good'
            userid_check = None
        elif review_status == 'complete-bad':
            check_review_status = 'complete-bad'
            userid_check = None

        elif review_status == 'self-all' and userid is not None:
            check_review_status = 'all'
            userid_check = (userid, 'include')
        elif review_status == 'self-incomplete' and userid is not None:
            check_review_status = 'incomplete'
            userid_check = (userid, 'include')
        elif review_status == 'self-complete-good' and userid is not None:
            check_review_status = 'complete-good'
            userid_check = (userid, 'include')
        elif review_status == 'self-complete-bad' and userid is not None:
            check_review_status = 'self-complete-bad'
            userid_check = (userid, 'include')

        elif review_status == 'other-all' and userid is not None:
            check_review_status = 'all'
            userid_check = (userid, 'exclude')
        elif review_status == 'other-incomplete' and userid is not None:
            check_review_status = 'incomplete'
            userid_check = (userid, 'exclude')
        elif review_status == 'other-complete-good' and userid is not None:
            check_review_status = 'complete-good'
            userid_check = (userid, 'exclude')
        elif review_status == 'other-complete-bad' and userid is not None:
            check_review_status = 'other-complete-bad'
            userid_check = (userid, 'exclude')

        # figure out the page slices by looking up the object count
        list_count = catalogs.get_object_count(
            (conn, meta),
            userid_check=userid_check,
            review_status=review_status,
        )
        if (list_count % max_objects):
            n_pages = int(list_count/max_objects) + 1
        else:
            n_pages = int(list_count/max_objects)

        # this returns a list of tuples (keyid, objectid)
        objectlist, ret_start_keyid, ret_end_keyid, revorder = (
            catalogs.get_objects(
                (conn, meta),
                userid_check=userid_check,
                review_status=check_review_status,
                start_keyid=start_keyid,
                end_keyid=end_keyid,
                max_objects=max_objects,
                getinfo='objectids',
                fast_fetch=True
            )
        )

        # reform to a single list
        returned_objectlist = sorted(list({x[1] for x in objectlist}))

        # this is the dict we return
        retdict = {
            'objectlist': returned_objectlist,
            'start_keyid':ret_start_keyid,
            'end_keyid':ret_end_keyid,
            'object_count':list_count,
            'rows_per_page':max_objects,
            'n_pages':n_pages,
        }

        return retdict

    except Exception:
        LOGGER.exception("Could not get object list.")
        if raiseonfail:
            raise

        return None


def worker_insert_object_comments(
        userid,
        username,
        comments,
        good_flags,
        max_good_votes,
        bad_flags,
        max_bad_votes,
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
            good_flags,
            max_good_votes,
            bad_flags,
            max_bad_votes,
            username=username,
        )

        # this is the dict we return
        retdict = {
            'updated': updated == 1,
        }

        return retdict

    except Exception:
        LOGGER.exception("Could not insert the comments into the DB.")
        if raiseonfail:
            raise
        return None


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
            - 'complete-good' -> objects that have at least 2 'good' votes
            - 'complete-bad' -> objects that have at least 2 'bad' votes
            - 'incomplete' -> objects that don't have 2 votes either way

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
                                     'incomplete',
                                     'complete-good',
                                     'complete-bad',
                                     'self-all',
                                     'self-incomplete',
                                     'self-complete-good',
                                     'self-complete-bad',
                                     'other-all',
                                     'other-incomplete',
                                     'other-complete-good',
                                     'other-complete-bad'):
                raise ValueError("Unknown review status requested: '%s'" %
                                 review_status)

            keytype = xhtml_escape(self.get_argument('keytype', 'start'))
            keyid = int(
                xhtml_escape(self.get_argument('keyid', '1'))
            )
            max_objects = self.siteinfo['rows_per_page']

            if keytype.strip() == 'start':

                objectlist_info = yield self.executor.submit(
                    worker_get_objects,
                    userid=self.current_user['user_id'],
                    review_status=review_status,
                    start_keyid=keyid,
                    end_keyid=None,
                    max_objects=max_objects,
                )

            elif keytype.strip() == 'end':

                objectlist_info = yield self.executor.submit(
                    worker_get_objects,
                    userid=self.current_user['user_id'],
                    review_status=review_status,
                    start_keyid=None,
                    end_keyid=keyid,
                    max_objects=max_objects,
                )

            else:

                objectlist_info = yield self.executor.submit(
                    worker_get_objects,
                    userid=self.current_user['user_id'],
                    review_status=review_status,
                    start_keyid=keyid,
                    end_keyid=None,
                    max_objects=max_objects,
                )

            # render the result
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

        except Exception:

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
                self.current_user['user_id'],
                objindex,
                self.basedir,
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

        except Exception:

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
                    self.current_user['user_id'],
                    objectid,
                    self.basedir,
                )

                # if this object actually exists and is writable, we can do
                # stuff on it
                if (objectinfo is not None and
                    objectinfo['review_status'] == 'incomplete' and
                    objectinfo['already_reviewed'] is False):

                    commentdict = {'objectid':objectid,
                                   'comment':comment_text,
                                   'user_flags':json.loads(user_flags)}

                    updated = yield self.executor.submit(
                        worker_insert_object_comments,
                        userid,
                        username,
                        commentdict,
                        [x.strip() for x in
                         self.siteinfo['good_flag_keys'].split(',')],
                        self.siteinfo['max_good_votes'],
                        [x.strip() for x in
                         self.siteinfo['bad_flag_keys'].split(',')],
                        self.siteinfo['max_bad_votes'],
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

                elif (objectinfo is not None and
                      objectinfo['already_reviewed'] is True):

                    retdict = {'status':'failed',
                               'message':(
                                   "You have already reviewed this object."
                               ),
                               'result':None}
                    self.write(retdict)
                    self.finish()

                else:

                    retdict = {'status':'failed',
                               'message':(
                                   "Object not found, or is already complete. "
                                   "Your comments were not saved."
                               ),
                               'result':None}
                    self.write(retdict)
                    self.finish()

            # if no comment text was supplied, do nothing
            else:

                retdict = {
                    'status':'ok',
                    'message':'No comments supplied. Object is unchanged.',
                    'result': None
                }
                self.write(retdict)
                self.finish()

        except Exception:

            LOGGER.exception('failed to save changes for object ID: %r' %
                             objectid)
            self.set_status(400)
            retdict = {'status':'failed',
                       'message':'Invalid save request for object ID',
                       'result':None}
            self.write(retdict)
            self.finish()
