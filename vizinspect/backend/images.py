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

def load_galaxy_image(object_index, basedir, images_subdir='images'):
    '''This loads a Galaxy image from a PNG into an array readable by matplotlib.

    Parameters
    ----------

    object_index: int
        The current object's source index in the input catalog.

    basedir : str
        The base directory that the viz-inspect server is working in.

    images_subdir : str
        The subdirectory under the `basedir` containing all of the images.

    Returns
    -------

    image : np.array
        This returns an image that's loadable directly using
        `matplotlib.pyplot.imshow`.

    '''

    image_fpath = os.path.join(basedir,
                               images_subdir,
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

def make_main_plot(catalog,
                   source_index,
                   basedir,
                   plot_fontsize=15,
                   images_subdir='images',
                   site_datadir='viz-inspect-data',
                   outfile=None):
    '''This generates the main plot.

    Parameters
    ----------

    catalog : pandas.DataFrame
        This is the catalog of objects loaded into a pandas dataframe.

    source_index : int
        The index of the object to make a plot for.

    basedir : str
        The base directory where the viz-inspect server is operating.

    plot_fontsize: int
        The font-size of the plot to make in points.

    images_subdir : str
        The subdirectory under the `basedir` where the object images are
        located.

    site_datadir : str
        The subdirectory under the `basedir` where the images to serve to the
        frontend will be placed.

    outfile : str or None
        The output PNG file name where the plot will be written to. If this is
        None, the plot will be written to a file called
        'current-object-plot.png' in the site-static directory so the
        viz-inspect server can load it into the interface. If this is a str,
        should be a path indicating where the output plot file will be written.

    Returns
    -------

    str
        The path where the plot was written to.

    '''

    plot_fontsize = 15
    fig = plt.figure(figsize=(10, 6))
    adjust = dict(wspace=0.13,
                  hspace=0.25,
                  bottom=0.1,
                  top=0.97,
                  right=0.96,
                  left=-0.02)
    grid = plt.GridSpec(2, 3, **adjust)
    ax_img = fig.add_subplot(
        grid[0:2, 0:2], xticks=[], yticks=[])
    ax_top = fig.add_subplot(grid[0,2])
    ax_bot = fig.add_subplot(grid[1,2])

    # add in the image of the object
    img = load_galaxy_image(source_index,
                            basedir,
                            images_subdir=images_subdir)

    # FIXME: check if the image's origin really is 0,0 in the bottom-left. If
    # not, can remove origin kwarg below.
    ax_img.imshow(img)

    # make the color plot
    ax_top.scatter(
        catalog['g-i'],
        catalog['g-r'],
        alpha=0.3,
        rasterized=True
    )
    ax_top.set_xlabel('$g-i$', fontsize=plot_fontsize)
    ax_top.set_ylabel('$g-r$', fontsize=plot_fontsize)
    ax_top.set_xlim(catalog['g-i'].min()-0.1,
                    catalog['g-i'].max()+0.1)
    ax_top.set_ylim(catalog['g-r'].min()-0.1,
                    catalog['g-r'].max()+0.1)

    # overplot this object as a star
    ax_top.scatter(catalog.loc[source_index,'g-i'],
                   catalog.loc[source_index,'g-r'],
                   c='k', s=300, marker='*', edgecolor='k')

    # make the half-light radius and surface-brightness plot
    ax_bot.scatter(
        catalog['r_e'],
        catalog.mu_e_ave_forced_g,
        alpha=0.3
    )
    ax_bot.set_xlabel(
        r'$r_\mathrm{eff}\ \mathrm{[arcsec]}$',
        fontsize=plot_fontsize)
    ax_bot.set_ylabel(
        r'$\langle\mu_e(g)\rangle\ \mathrm{[mag/arcsec^2]}$',
        fontsize=plot_fontsize)
    ax_bot.set_xlim(0, 16)
    ax_bot.set_ylim(24, 29)

    # overplot this object as a star
    ax_bot.scatter(catalog.loc[source_index,'r_e'],
                   catalog.loc[source_index,'mu_e_ave_forced_g'],
                   c='k', s=300, marker='*', edgecolor='k')

    if outfile is None:

        outfile = os.path.join(basedir,
                               site_datadir,
                               'current-object-plot.png')

    fig.savefig(outfile,dpi=100)
    plt.close('all')

    return outfile
