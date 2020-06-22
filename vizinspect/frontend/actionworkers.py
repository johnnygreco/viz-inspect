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


#############
## IMPORTS ##
#############

from ..backend import catalogs


######################
## WORKER FUNCTIONS ##
######################

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

        #
        # for all objects
        #

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

        #
        # for all objects that have votes from this user
        #

        elif review_status == 'self-incomplete' and userid is not None:
            LOGGER.info("self-incomplete requested for userid: %s" % userid)
            check_review_status = 'incomplete'
            userid_check = (userid, 'include')

        elif review_status == 'self-complete-good' and userid is not None:
            LOGGER.info("self-complete-good requested for userid: %s" % userid)
            check_review_status = 'complete-good'
            userid_check = (userid, 'include')

        elif review_status == 'self-complete-bad' and userid is not None:
            LOGGER.info("self-complete-bad requested for userid: %s" % userid)
            check_review_status = 'complete-bad'
            userid_check = (userid, 'include')

        #
        # for all objects that have votes from others but not this user
        #

        elif review_status == 'other-incomplete' and userid is not None:
            LOGGER.info("other-incomplete requested for userid: %s" % userid)
            check_review_status = 'incomplete'
            userid_check = (userid, 'exclude')

        #
        # now, do the operations
        #

        # figure out the page slices by looking up the object count
        list_count = catalogs.get_object_count(
            (conn, meta),
            userid_check=userid_check,
            review_status=check_review_status,
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
            'review_status':review_status,
        }

        return retdict

    except Exception:
        LOGGER.exception("Could not get object list.")
        if raiseonfail:
            raise

        return None


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


def worker_insert_object_comments(
        userid,
        username,
        comments,
        good_flags,
        max_good_votes,
        bad_flags,
        max_bad_votes,
        max_all_votes,
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
            max_all_votes,
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
