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

from .actionworkers import (
    worker_get_object,
    worker_get_objects,
    worker_insert_object_comments,
)


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
            - 'self-complete-good' -> this user's voted objects good-complete
            - 'self-complete-bad' -> this user's voted objects bad-complete
            - 'self-incomplete' -> this user's voted objects incomplete
            - 'other-complete-good' -> other users' voted objects good-complete
            - 'other-complete-bad' -> other users' voted objects bad-complete
            - 'other-incomplete' -> other users' voted objects incomplete

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
                                     'self-incomplete',
                                     'self-complete-good',
                                     'self-complete-bad',
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
                    review_status=review_status,
                    userid=self.current_user['user_id'],
                    start_keyid=keyid,
                    end_keyid=None,
                    max_objects=max_objects,
                )

            elif keytype.strip() == 'end':

                objectlist_info = yield self.executor.submit(
                    worker_get_objects,
                    review_status=review_status,
                    userid=self.current_user['user_id'],
                    start_keyid=None,
                    end_keyid=keyid,
                    max_objects=max_objects,
                )

            else:

                objectlist_info = yield self.executor.submit(
                    worker_get_objects,
                    review_status=review_status,
                    userid=self.current_user['user_id'],
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

            # check if there's more than one flag selected
            user_flags = json.loads(user_flags)
            if sum(user_flags[k] for k in user_flags) > 1:
                LOGGER.error(
                    "More than one flag is selected for "
                    "object: %s, userid: %s" %
                    (objectid, self.current_user['user_id'])
                )
                retdict = {
                    'status':'failed',
                    'result':None,
                    'message':(
                        "You can't choose more than one flag per object."
                    )
                }
                self.write(retdict)
                raise web.Finish()

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

                if (objectinfo is None):
                    LOGGER.error("Object: %s doesn't exist (userid: %s)" %
                                 (objectid, self.current_user['user_id']))
                    retdict = {
                        'status':'failed',
                        'result':None,
                        'message':(
                            "You can't choose more than one flag per object."
                        )
                    }
                    self.write(retdict)
                    self.finish()

                elif (objectinfo is not None and
                      objectinfo['already_reviewed'] is True):

                    LOGGER.error(
                        "Object: %s has been already reviewed by userid: %s" %
                        (objectid, self.current_user['user_id'])
                    )
                    retdict = {
                        'status':'failed',
                        'result':None,
                        'message':(
                            "You have already reviewed this object."
                        )
                    }
                    self.write(retdict)
                    self.finish()

                elif (objectinfo is not None and
                      objectinfo['already_reviewed'] is False and
                      objectinfo['review_status'] == 'incomplete'):

                    commentdict = {'objectid':objectid,
                                   'comment':comment_text,
                                   'user_flags':user_flags}

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

                        LOGGER.info(
                            "Object: %s successfully "
                            "reviewed by userid: %s: %r" %
                            (objectid,
                             self.current_user['user_id'],
                             commentdict)
                        )

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
                                   "Object not found, or is already complete. "
                                   "Your comments were not saved."
                               ),
                               'result':None}
                    self.write(retdict)
                    self.finish()

            # if no comment content was supplied, do nothing
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
