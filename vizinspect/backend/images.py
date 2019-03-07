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


import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams['font.family'] = 'serif'
matplotlib.rcParams['mathtext.fontset'] = 'cm'
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
from matplotlib.figure import Figure



###########################
## LOADING GALAXY IMAGES ##
###########################

def load_galaxy_image(object_index, basedir, source_subdir='images'):
    '''This loads a Galaxy image from a PNG into an array readable by matplotlib.

    Parameters
    ----------

    object_index: int
        The current object's source index in the input catalog.

    basedir : str
        The base directory that the viz-inspect server is working in.

    source_subdir : str
        The subdirectory under the `basedir` containing all of the images.

    Returns
    -------

    image : np.array or PIL Image
        This returns an image that's loadable directly using
        `matplotlib.pyplot.imshow`.

    '''

    image_fpath = os.path.join(basedir,
                               source_subdir,
                               'candy-{}.png'.format(object_index))

    try:
        image = mpimg.imread(image_fpath)
        return image

    except Exception as e:
        LOGEXCEPTION('could not load the requested image: %s' % image_fpath)
        return None



##################
## MAKING PLOTS ##
##################
