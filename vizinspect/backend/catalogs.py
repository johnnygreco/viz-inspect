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


#########################
## LOADING THE CATALOG ##
#########################

def load_catalog(catalog_fpath,
                 review_mode=False,
                 flags_to_use=('candy','junk','tidal','cirrus'),
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

    pdkwargs : extra keyword arguments
        All of these will passed directly to the `pandas.read_csv` function.

    Returns
    -------

    pandas DataFrame
        A pandas DataFrame with the catalog contents is returned.

    '''

    catalog = pd.read_csv(catalog_fpath, **pdkwargs)

    # add in the extra columns we need
    if not review_mode:

        # add in the colors columns
        catalog['g-i'] = catalog.m_tot_forced_g - catalog.m_tot
        catalog['g-i'] = catalog['g-i'] - catalog.A_g + catalog.A_i
        catalog['g-r'] = catalog.m_tot_forced_g - catalog.m_tot_forced_r
        catalog['g-r'] = catalog['g-r'] - catalog.A_g + catalog.A_r

        # add the flag columns to the catalog
        for flag in flags_to_use:
            catalog[flag] = -1

        # add the notes column
        catalog['notes'] = "no notes yet for this object."

    return catalog



def save_catalog(catalog,
                 catalog_fpath,
                 indexcol=False,
                 overwrite=False,
                 **pdkwargs):
    '''
    This writes the catalog to the given path.

    Parameters
    ----------

    catalog : pd.DataFrame
        This is an existing catalog loaded into a pandas DataFrame.

    catalog_fpath: str
        The CSV file to write the catalog to.

    indexcol : bool
        If True, will also write the index column to the CSV.

    overwrite: bool
        If True, will overwrite an existing catalog at the given path.

    pdkwargs : extra keyword arguments
        These will be passed directly to the `DataFrame.to_csv` method.

    Returns
    -------

    bool
        If True, file writing was successful. If not, it failed.

    '''

    if os.path.exists(os.path.abspath(catalog_fpath)) and not overwrite:
        LOGERROR(
            'overwrite = False and catalog exists at: %s, not overwriting' %
            catalog_fpath
        )
        return False

    else:
        return catalog.to_csv(catalog_fpath, index=indexcol, **pdkwargs)
