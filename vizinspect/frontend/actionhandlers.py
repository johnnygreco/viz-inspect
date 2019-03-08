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


###################
## LOCAL IMPORTS ##
###################

from .basehandler import BaseHandler

from ..backend import catalogs, images


######################
## WORKER FUNCTIONS ##
######################

def _load_object_from_catalog(
        catalog_csv,
        source_index,
        basedir,
        review_mode=False,
        flags_to_use=('candy','junk','tidal','cirrus'),
        load_comments=True,
        plot_fontsize=15,
        images_subdir='images',
        site_datadir='viz-inspect-data',
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

    cat, comm = catalogs.load_catalog(catalog_csv,
                                      review_mode=review_mode,
                                      flags_to_use=flags_to_use,
                                      load_comments=load_comments)

    if source_index > len(cat):
        LOGGER.error(
            'source index requested: %s is > len(catalog) = %s' % (
                source_index,
                len(cat)
            )
        )
        return None

    #
    # get the plot for the object
    #

    plotfile = os.path.join(
        basedir,
        images_subdir,
        '{catalog_fname}-{index}-plot.png'.format(
            catalog_fname=os.path.splitext(os.path.basename(catalog_csv))[0],
            index=source_index
        )
    )

    if not os.path.exists(plotfile):

        try:
            images.make_main_plot(
                cat,
                source_index,
                basedir,
                plot_fontsize=plot_fontsize,
                images_subdir=images_subdir,
                site_datadir=site_datadir,
                outfile=plotfile
            )

        except Exception as e:
            LOGGER.exception('could not make plot for '
                             'source: %s in catalog: %s'
                             % (source_index, catalog_csv))
            plotfile = None

    #
    # get the comments for the object
    #
    object_comments = comm[comm['source_index'] == source_index]

    # this is the dict we return
    retdict = {
        'info': cat.iloc[source_index],
        'plot':plotfile,
        'comments':object_comments
    }

    return retdict



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
                   flags_to_use,
                   file_lock):
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
        self.flags_to_use = flags_to_use

        # this is a tornado.locks Lock to serialize access to the CSV files on
        # disk
        self.file_lock = file_lock


    @gen.coroutine
    def get(self, source_index):
        '''This handles GET requests to the /api/load-object/<index> endpoint.

        Gets catalog and comment info, plots the object if not already plotted,
        and then returns JSON with everything.

        '''

        try:

            objindex = int(xhtml_escape(source_index))
            if objindex < 0:
                objindex = 0

            objectinfo = yield self.executor.submit(
                _load_object_from_catalog,
                self.catalog_csv,
                source_index,
                self.basedir,
                review_mode=False,
                flags_to_use=self.flags_to_use,
                load_comments=True,
                plot_fontsize=15,
                images_subdir=self.siteinfo['images_subdir'],
                site_datadir=self.siteinfo['data_path']
            )

            retdict = {'status':'ok',
                       'message':'object found OK',
                       'result':objectinfo}

            self.write(retdict)
            self.finish()

        except Exception as e:

            LOGGER.exception('failed to get requested source_index')
            self.set_status(400)
            retdict = {'status':'failed',
                       'message':'Invalid request for object index.',
                       'result':None}
            self.write(retdict)
            self.finish()



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
                   comment_csv,
                   file_lock):
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

        # this is a tornado.locks Lock to serialize access to the CSV files on
        # disk
        self.file_lock = file_lock


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
