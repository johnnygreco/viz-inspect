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


###################
## LOCAL IMPORTS ##
###################

from .basehandler import BaseHandler



#####################
## MAIN INDEX PAGE ##
#####################

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
                   cachedir,
                   catalog_csv,
                   comments_csv):
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

        self.catalog_csv = catalog_csv
        self.comments_csv = comments_csv


    @gen.coroutine
    def get(self, source_index):
        '''This handles GET requests to the /api/load-object/<index> endpoint.

        Gets catalog and comment info, plots the object if not already plotted,
        and then returns JSON with everything.


        '''

        self.render(
            'index.html',
            flash_messages=self.render_flash_messages(),
            user_account_box=self.render_user_account_box(),
            page_title='viz-inspect',
            siteinfo=self.siteinfo,
            current_user=self.current_user,
        )



class SaveObjectHandler(BaseHandler):
    '''This handles the /api/save-object endpoint.

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
                   cachedir,
                   catalog_csv,
                   comment_csv):
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

        self.catalog_csv = catalog_csv
        self.comment_csv = comment_csv


    @gen.coroutine
    def post(self):
        '''This handles POST requests to the /api/save-object endpoint.

        This only saves the current object.

        '''

        self.render(
            'index.html',
            flash_messages=self.render_flash_messages(),
            user_account_box=self.render_user_account_box(),
            page_title='viz-inspect',
            siteinfo=self.siteinfo,
            current_user=self.current_user,
        )
