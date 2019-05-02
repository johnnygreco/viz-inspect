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
import pathlib
import pickle


import numpy as np
import numpy.random as npr

import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams['font.family'] = 'serif'
matplotlib.rcParams['mathtext.fontset'] = 'cm'
import matplotlib.image as mpimg
import matplotlib.pyplot as plt

from vizinspect import bucketstorage
from .catalogs import get_object, get_objects



###########################
## LOADING GALAXY IMAGES ##
###########################

def load_galaxy_image(image_file,
                      local_imgdir,
                      bucket_client=None):
    '''This loads a Galaxy image from a PNG into an array
    readable by matplotlib.

    If `image_file` starts with dos:// or s3://, this function will assume you
    mean to download images from a remote bucket. It will then do the following:

    - check for the image_file's existence in local_imgdir
    - if found, will load it from there
    - if not found, will download it from the specified bucket URL (this should
      be in the image_file) and write it to the local_imgdir. This requires a
      valid `bucketstorage.client` in `bucket_client`.

    Parameters
    ----------

    image_file : str
        The name of the file to load.

    local_imgdir : str
        The local image directory to check for images in and to write them to.

    bucket_client : bucketstorage.client instance
        This is a client to connect to S3/DOS buckets and download files.

    Returns
    -------

    image : np.array
        This returns an image that's loadable directly using
        `matplotlib.pyplot.imshow`.

    '''

    # check if the image is remote
    if image_file.startswith('dos://') or image_file.startswith('s3://'):

        bucket_imagepath = image_file.replace('dos://','').replace('s3://','')
        bucket_name = os.path.dirname(bucket_imagepath)
        file_name = os.path.basename(bucket_imagepath)
        download_to = os.path.abspath(os.path.join(local_imgdir, file_name))

        if os.path.exists(download_to):

            use_image_file = download_to
            LOGINFO('Using local cached copy of %s.' % image_file)


        else:

            use_image_file = bucketstorage.get_file(
                bucket_name,
                file_name,
                download_to,
                client=bucket_client
            )
            LOGINFO('Downloaded %s from remote.' % image_file)

    else:

        use_image_file = image_file


    # finally, load the image
    try:

        image = mpimg.imread(use_image_file)

        # touch this file so we know it was recently accessed and won't get
        # evicted from the cache if it's accessed often
        pathlib.Path(use_image_file).touch()

        return image

    except Exception as e:

        LOGEXCEPTION('could not load the requested image: %s' % image_file)
        return None



##################
## MAKING PLOTS ##
##################

def make_main_plot(
        objectid,
        dbinfo,
        outdir,
        plot_fontsize=15,
        color_plot_xlim=None,
        color_plot_ylim=None,
        reff_plot_xlim=None,
        reff_plot_ylim=None,
        random_sample=None,
        random_sample_percent=2.0,
        save_random_sample=True,
        bucket_client=None,
):
    '''This generates the main plot.

    Parameters
    ----------

    objectid : int
        The objectid of the object to make the plot for.

    dbinfo : tuple
        This is a tuple of two items:

        - the database URL or the connection instance to use
        - the database metadata object

        If the database URL is provided, a new engine will be used. If the
        connection itself is provided, it will be re-used.

    outdir : str
        The directory where the plot file will be written. This is also the
        directory where images will be downloaded from a remote bucket.

    plot_fontsize: int
        The font-size of the plot to make in points.

    color_plot_xlim : tuple of two ints or None
        This sets the xlim of the color-color plot.

    color_plot_ylim : tuple of two ints or None
        This sets the ylim of the color-color plot.

    reff_plot_xlim : tuple of two ints or None
        This sets the xlim of the reff-mu plot.

    reff_plot_ylim : tuple of two ints or None
        This sets the ylim of the reff-mu plot.

    random_sample : int
        The number of objects to sample randomly from the database to make the
        plot.

    random_sample_percent: float or None
        If this is provided, will be used preferentially over `random_sample` to
        push the random sampling into the Postgres database itself. This must be
        a float between 0.0 and 100.0 indicating the percentage of rows to
        sample.

    save_random_sample : bool
        This saves the random sample to a pickle file. If
        `random_sample_percent` is not None on a subsequent call to this
        function, this function will attempt to load the random sample from the
        saved pickle and use that instead of doing another resample. This should
        save time when plotting.

    bucket_client : bucketstorage.client instance
        This is a client used to download files from S3/DOS.

    Returns
    -------

    str
        The path of the file where the plot was written to.

    '''

    # get this object's info
    this_object = get_object(objectid, dbinfo)

    if not this_object or len(this_object) == 0:
        LOGERROR("No information found for objectid: %s" % objectid)
        return None

    this_gi_color = this_object[0]['extra_columns']['g-i']
    this_gr_color = this_object[0]['extra_columns']['g-r']
    this_r_e = this_object[0]['extra_columns']['flux_radius_ave_g']
    this_mu_e_ave_forced_g = (
        this_object[0]['extra_columns']['mu_ave_g']
    )
    this_object_image = this_object[0]['filepath']

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
    img = load_galaxy_image(this_object_image,
                            outdir,
                            bucket_client=bucket_client)

    if img is not None:

        # FIXME: check if the image's origin really is 0,0 in the
        # bottom-left. If not, can remove origin kwarg below.
        ax_img.imshow(img)

    if random_sample_percent is not None:

        random_sample = None

        # check for the existence of the sample pickle
        sample_picklef = os.path.join(
            outdir,
            'random-sample-percent-%.1f.pkl' % random_sample_percent
        )

        if os.path.exists(sample_picklef):

            LOGINFO("Using cached random sample from %s" % sample_picklef)

            with open(sample_picklef,'rb') as infd:
                gi_color, gr_color, r_e, mu_e_ave_forced_g = pickle.load(
                    infd
                )

        else:

            # get the info from the database
            full_catalog, start_keyid, end_keyid = (
                get_objects(
                    dbinfo,
                    getinfo='plotcols',
                    end_keyid=None,
                    random_sample_percent=random_sample_percent
                )
            )

            gi_color = np.array([x[0] for x in full_catalog])
            gr_color = np.array([x[1] for x in full_catalog])
            r_e = np.array([x[2] for x in full_catalog])
            mu_e_ave_forced_g = np.array([x[3] for x in full_catalog])

            # write the random sampled arrays to the pickle file
            with open(sample_picklef,'wb') as outfd:
                pickle.dump((gi_color, gr_color, r_e, mu_e_ave_forced_g),
                            outfd,
                            pickle.HIGHEST_PROTOCOL)


    # this is using numpy sampling
    if random_sample is not None:

        sample_index = npr.choice(
            gi_color.size,
            random_sample,
            replace=False
        )

        sampled_gi_color = gi_color[sample_index]
        sampled_gr_color = gr_color[sample_index]
        sampled_re = r_e[sample_index]
        sampled_mue = mu_e_ave_forced_g[sample_index]

    else:

        sampled_gi_color = gi_color
        sampled_gr_color = gr_color
        sampled_re = r_e
        sampled_mue = mu_e_ave_forced_g


    # make the color plot for all of the objects
    ax_top.plot(
        sampled_gi_color,
        sampled_gr_color,
        alpha=0.3,
        rasterized=True,
        linestyle='None',
        marker='.',
        ms=1,
    )
    ax_top.set_xlabel('$g-i$', fontsize=plot_fontsize)
    ax_top.set_ylabel('$g-r$', fontsize=plot_fontsize)

    if color_plot_xlim is not None:
        ax_top.set_xlim(color_plot_xlim)
    else:
        ax_top.set_xlim(gi_color.min()-0.2,
                        gi_color.max()+0.2)

    if color_plot_ylim is not None:
        ax_top.set_ylim(color_plot_ylim)
    else:
        ax_top.set_ylim(gr_color.min()-0.2,
                        gr_color.max()+0.2)

    # overplot the current object as a star
    ax_top.plot(
        this_gi_color,
        this_gr_color,
        linestyle='None',
        ms=20,
        markeredgecolor='k',
        markerfacecolor='k',
        marker='*'
    )

    # make the half-light radius and surface-brightness plot
    ax_bot.plot(
        sampled_re,
        sampled_mue,
        alpha=0.3,
        rasterized=True,
        linestyle='None',
        marker='.',
        ms=1,
    )
    ax_bot.set_xlabel(
        r'$r_\mathrm{eff}\ \mathrm{[arcsec]}$',
        fontsize=plot_fontsize)
    ax_bot.set_ylabel(
        r'$\langle\mu_e(g)\rangle\ \mathrm{[mag/arcsec^2]}$',
        fontsize=plot_fontsize)

    if reff_plot_xlim is not None:
        ax_bot.set_xlim(reff_plot_xlim)
    else:
        ax_bot.set_xlim(0,20)

    if reff_plot_ylim is not None:
        ax_bot.set_ylim(reff_plot_ylim)
    else:
        ax_bot.set_ylim(20,30)

    # overplot this object as a star
    ax_bot.plot(
        this_r_e,
        this_mu_e_ave_forced_g,
        linestyle='None',
        ms=20,
        markeredgecolor='k',
        markerfacecolor='k',
        marker='*'
    )

    outfile = os.path.join(
        outdir,
        'plot-objectid-{objectid}.png'.format(
            objectid=objectid
        )
    )

    fig.savefig(outfile,dpi=100)
    plt.close('all')

    return outfile
