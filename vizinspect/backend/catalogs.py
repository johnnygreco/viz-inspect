#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''{{ name }}.py - {{ author }} ({{ email }}) - {{ month }} {{ year }}
License: MIT - see the LICENSE file for the full text.

'''

#############
## LOGGING ##
#############

import logging
from vizinspect import log_sub, log_fmt, log_date_fmt

DEBUG = False
if DEBUG:
    level = logging.DEBUG
else:
    level = logging.INFO
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=level,
    style=log_sub,
    format=log_fmt,
    datefmt=log_date_fmt,
)

LOGDEBUG = LOGGER.debug
LOGINFO = LOGGER.info
LOGWARNING = LOGGER.warning
LOGERROR = LOGGER.error
LOGEXCEPTION = LOGGER.exception

#############
## IMPORTS ##
#############

import os.path
import pandas as pd
from datetime import datetime


#########################
## LOADING THE CATALOG ##
#########################

def load_catalog(catalog_fpath,
                 review_mode=False,
                 flags_to_use=('candy','junk','tidal','cirrus'),
                 load_comments=True,
                 **pdkwargs):
    '''This loads the catalog into a CSV file.

    Parameters
    ----------

    catalog_fpath : str
        The path to the CSV to load.

    review_mode : bool
        If False, doesn't assume the input catalog has been opened before,
        therefore doesn't have all the extra cols we add after loading it for
        the first time. The loaded catalog will then have various columns added
        to it. If True, will open the catalog and return it directly and not
        attempt to add the extra columns.

    flags_to_use : sequence of str
        These are the flag column names to add into the catalog when loading it
        for the first time.

    load_comments : bool
        If this is True, will load comments from the
        `{catalog_basename}-comments.csv` file associated with this catalog. The
        comments CSV is a file containing the following columns::

            source_index
            user
            datetime_utc
            comment_text

        In this way, we can load multiple people's comments for each object in
        the current catalog.

    pdkwargs : extra keyword arguments
        All of these will passed directly to the `pandas.read_csv` function.

    Returns
    -------

    pandas DataFrame, comments DataFrame : tuple
        A tuple of two DataFrames is returned:

        - A pandas DataFrame with the catalog contents
        - A pandas DataFrame with the catalog comments or None if
          `load_comments` is False

    '''

    # read the catalog
    catalog = pd.read_csv(catalog_fpath, **pdkwargs)

    # get the associated comments file
    comment_csv_file = '{basename}-comments.csv'.format(
        basename=os.path.splitext(os.path.basename(catalog_fpath))[0]
    )
    comment_csv_fpath = os.path.join(os.path.dirname(catalog_fpath),
                                     comment_csv_file)

    if os.path.exists(comment_csv_fpath) and load_comments:

        comments = pd.read_csv(comment_csv_fpath, **pdkwargs)

    elif (not os.path.exists(comment_csv_fpath)) and load_comments:

        comments = {'source_index':[0],
                    'user':[''],
                    'datetime_utc':[datetime.utcnow()],
                    'comment_text':['']}
        comments = pd.DataFrame(comments)

    else:
        comments = None

    # add in the extra columns we need if we're loading this catalog for the
    # first time
    if not review_mode:

        # add in the colors columns
        catalog['g-i'] = catalog.m_tot_forced_g - catalog.m_tot
        catalog['g-i'] = catalog['g-i'] - catalog.A_g + catalog.A_i
        catalog['g-r'] = catalog.m_tot_forced_g - catalog.m_tot_forced_r
        catalog['g-r'] = catalog['g-r'] - catalog.A_g + catalog.A_r

        # add the flag columns to the catalog
        for flag in flags_to_use:
            catalog[flag] = -1

    return catalog, comments



def save_catalog(catalog,
                 basedir,
                 catalog_fpath=None,
                 save_comments=None,
                 indexcol=False,
                 overwrite=False,
                 **pdkwargs):
    '''This writes the catalog to the given path.

    Parameters
    ----------

    catalog : pd.DataFrame
        This is an existing catalog loaded into a pandas DataFrame.

    basedir : str
        The base directory for the viz-inspect server.

    catalog_fpath: str or None
        The CSV file to write the catalog to. If this is None, will write the
        catalog to a file called reviewed-catalog.csv in the `basedir`.

    save_comments : pandas.DataFrame or None
        This is the comments table associated with the objects in the current
        catalog. If this is provided, the comments table will be saved to a CSV
        file alongside the catalog.

    indexcol : bool
        If True, will also write the index column to the CSV.

    overwrite: bool
        If True, will overwrite an existing catalog at the given path.

    pdkwargs : extra keyword arguments
        These will be passed directly to the `DataFrame.to_csv` method.

    Returns
    -------

    (catalog_file, comments_file) : tuple
        The name of the catalog file written. Optionally, the name of the
        comments file written if `save_comments` was a pd.DataFrame, None if
        this was not provided..

    '''

    if not catalog_fpath:
        catalog_fpath = os.path.join(basedir,'reviewed-catalog.csv')

    if (catalog_fpath and
        os.path.exists(os.path.abspath(catalog_fpath)) and
        not overwrite):
        LOGERROR(
            'overwrite = False and catalog exists at: %s, not overwriting' %
            catalog_fpath
        )
        return None, None

    else:

        catalog.to_csv(catalog_fpath, index=indexcol, **pdkwargs)

        if isinstance(save_comments, pd.DataFrame):

            # get the associated comments file
            comment_csv_file = '{basename}-comments.csv'.format(
                basename=os.path.splitext(os.path.basename(catalog_fpath))[0]
            )
            comment_csv_fpath = os.path.join(os.path.dirname(catalog_fpath),
                                             comment_csv_file)


            save_comments.to_csv(comment_csv_fpath,
                                 index=indexcol,
                                 **pdkwargs)

            return catalog_fpath, comment_csv_fpath

        else:

            return catalog_fpath, None
