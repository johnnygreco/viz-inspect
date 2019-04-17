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
import numpy as np
from datetime import datetime
import os.path
import multiprocessing as mp

# for generating encrypted token information
from cryptography.fernet import Fernet


######################################
## CUSTOM JSON ENCODER FOR FRONTEND ##
######################################

# we need this to send objects with the following types to the frontend:
# - bytes
# - ndarray
import json

class FrontendEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, np.ndarray):
            return obj.tolist()
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

def worker_get_object(objectid, basedir, userid):
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

        currproc = mp.current_process()
        conn, meta = currproc.connection, currproc.metadata

        # this returns a list of objectinfo rows
        # one row per entry in the comments table for this object
        # we'll reform everything to a single dict suitable for JSON output
        # and turn the comments into a row of dicts per commenter
        objectinfo = catalogs.get_object(objectid,
                                         (conn, meta))

        comments = [{'comment_added_on':x['comment_added_on'],
                     'comment_by_userid':x['comment_by_userid'],
                     'comment_by_username':x['comment_by_username'],
                     'comment_userset_flags':x['comment_userset_flags'],
                     'comment_text':x['comment_text']} for x in objectinfo]

        comments = sorted(comments,
                          key=lambda x: x['comment_added_on'],
                          reverse=True)


        objectinfo_dict = objectinfo[0]
        del objectinfo_dict['comment_added_on']
        del objectinfo_dict['comment_by_userid']
        del objectinfo_dict['comment_by_username']
        del objectinfo_dict['comment_userset_flags']
        del objectinfo_dict['comment_text']
        objectinfo_dict['filepath'] = 'redacted'

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
                os.path.join(basedir, 'viz-inspect-data')
            )
            objectplot = os.path.abspath(made_plot)

        # set the readonly flag
        if (objectinfo_dict['reviewer_userid'] is not None and
            userid == objectinfo_dict['reviewer_userid']):
            readonly = False
        elif (objectinfo_dict['reviewer_userid'] is not None and
              userid != objectinfo_dict['reviewer_userid']):
            readonly = True
        elif (objectinfo_dict['reviewer_userid'] is None):
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

        return retdict

    except Exception as e:
        LOGGER.exception("Could not get info for object: %s" % objectid)
        return None


def worker_get_objects(review_status='all',
                       userid=None,
                       start_keyid=0,
                       end_keyid=50,
                       allinfo=False):
    '''
    This returns the full object list.

    '''

    try:

        currproc = mp.current_process()
        conn, meta = currproc.connection, currproc.metadata

        # this returns a list of dicts {'objectid': <objectid>}
        objectlist, ret_start_keyid, ret_end_keyid = catalogs.get_objects(
            (conn, meta),
            review_status=review_status,
            userid=userid,
            start_keyid=start_keyid,
            end_keyid=end_keyid,
            allinfo=allinfo
        )

        # reform to a single list
        returned_objectlist = [x['objectid'] for x in objectlist]

        # this is the dict we return
        retdict = {
            'objectlist': list(set(returned_objectlist)),
            'start_keyid':ret_start_keyid,
            'end_keyid':ret_end_keyid,
        }

        return retdict

    except Exception as e:
        LOGGER.exception("Could not get object list.")
        return None



def worker_insert_object_comments(
        userid,
        username,
        comments,
):
    '''
    This inserts object comments.

    '''

    try:

        currproc = mp.current_process()
        conn, meta = currproc.connection, currproc.metadata

        # this returns a list of dicts {'objectid': <objectid>}
        updated = catalogs.insert_object_comments(
            userid,
            comments,
            (conn, meta),
            username=username,
        )

        # reform to a single list
        objectid = updated[0]['objectid']
        objectinfo_dict = updated[0]

        all_comments = [{'comment_added_on':x['comment_added_on'],
                         'comment_by_userid':x['comment_by_userid'],
                         'comment_by_username':x['comment_by_username'],
                         'comment_userset_flags':x['comment_userset_flags'],
                         'comment_text':x['comment_text']} for x in updated]

        all_comments = sorted(all_comments,
                              key=lambda x: x['comment_added_on'],
                              reverse=True)

        del objectinfo_dict['comment_added_on']
        del objectinfo_dict['comment_by_userid']
        del objectinfo_dict['comment_by_username']
        del objectinfo_dict['comment_userset_flags']
        del objectinfo_dict['comment_text']
        objectinfo_dict['filepath'] = 'redacted'

        # get the plot if it exists
        objectplot = 'plot-objectid-{objectid}.png'.format(objectid=objectid)

        # set the readonly flag
        if (objectinfo_dict['reviewer_userid'] is not None and
            userid == objectinfo_dict['reviewer_userid']):
            readonly = False
        elif (objectinfo_dict['reviewer_userid'] is not None and
              userid != objectinfo_dict['reviewer_userid']):
            readonly = True
        elif (objectinfo_dict['reviewer_userid'] is None):
            readonly = False
        else:
            readonly = True

        # this is the dict we return
        retdict = {
            'info': objectinfo_dict,
            'plot':os.path.basename(objectplot),
            'comments':all_comments,
            'readonly':readonly
        }

        return retdict

    except Exception as e:
        LOGGER.exception("Could not insert the comments into the DB.")
        return None



def worker_update_object_flags(
        objectid,
        flags,
):
    '''
    This updates object flags.

    '''



def worker_export_catalog(
        basedir,
        outdir='viz-inspect-data',
):
    '''This exports the catalog from the DB to the output dir.

    By default the file is written to the viz-inspect-data dir under the
    basedir. This allows the server to serve it back to the client if they want
    to download it after exporting it.

    '''



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
            - 'unreviewed-self' -> objects reviewed by this user
            - 'unreviewed-other' -> objects reviewed by other users

            For -self retrieval types, we'll get the userid out of the session
            dict.

        start_keyid : int, optional, default = 0
            The first object keyid to retrieve. Useful for pagination.

        end_keyid : int, optional, default = 50
            The last object keyid to retrieve. Useful for pagination.

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
            start_keyid = xhtml_escape(self.get_argument('start_keyid', '0'))
            end_keyid = xhtml_escape(self.get_argument('end_keyid', '50'))

            start_keyid = int(start_keyid)
            if end_keyid == 'none':
                end_keyid = None
            else:
                end_keyid = int(end_keyid)

            objectlist_info = yield self.executor.submit(
                worker_get_objects,
                review_status=review_status,
                userid=self.current_user['user_id'],
                start_keyid=start_keyid,
                end_keyid=end_keyid,
                allinfo=False
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

            objectinfo = yield self.executor.submit(
                worker_get_object,
                objindex,
                self.basedir,
                self.current_user['user_id'],
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
