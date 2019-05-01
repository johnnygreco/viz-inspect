#!/usr/bin/env python
# -*- coding: utf-8 -*-
# dospaces.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Apr 2019
# License: MIT - see the LICENSE file for the full text.

"""
This contains functions that handle Digital Ocean Spaces operations.

"""

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
import os
import boto3


#######################
## SPACES OPERATIONS ##
#######################

def client(keyfile,
           region='sfo2',
           endpoint='https://sfo2.digitaloceanspaces.com'):
    '''This makes a new DO Spaces client.

    Requires a keyfile containing the access token and the secret key in the
    following format::

        access_token secret_key

    '''

    with open(keyfile,'r') as infd:
        access_token, secret_key = infd.read().strip('\n').split()

    session = boto3.Session()
    client = session.client(
        's3',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_token,
        aws_secret_access_key=secret_key
    )
    return client


def list_buckets(
        client=None,
        keyfile=None,
        region=None,
        endpoint=None,
        raiseonfail=False,
):
    '''
    This lists all buckets in the region.

    '''

    if not client:
        client = client(keyfile, region=region, endpoint=endpoint)

    try:
        return client.list_buckets().get('Buckets')
    except Exception as e:

        LOGEXCEPTION("Could not list buckets.")

        if raiseonfail:
            raise

        return None



def list_bucket_contents(
        bucket,
        maxobjects=100,
        startingkey=None,
        keyprefix=None,
        client=None,
        keyfile=None,
        region=None,
        endpoint=None,
        raiseonfail=False,
):
    '''
    This lists a bucket's contents.

    '''

    if not client:
        client = client(keyfile, region=region, endpoint=endpoint)

    try:

        if not startingkey:
            startingkey = ''
        if not keyprefix:
            keyprefix = ''

        # DO uses v1 of the list_objects protocol
        ret = client.list_objects(
            Bucket=bucket,
            MaxKeys=maxobjects,
            Prefix=keyprefix,
            Marker=startingkey
        )
        content_list = ret.get('Contents')
        return content_list

    except Exception as e:

        LOGEXCEPTION("Could not list buckets.")

        if raiseonfail:
            raise

        return None



def get_file(
        bucket,
        filename,
        local_file,
        client=None,
        keyfile=None,
        region=None,
        endpoint=None,
        raiseonfail=False
):

    """This gets a file from an DOS bucket.

    Parameters
    ----------

    bucket : str
        The DOS bucket name.

    filename : str
        The full filename of the file to get from the bucket

    local_file : str
        Path to where the downloaded file will be stored.

    client : boto3.Client or None
        If None, this function will instantiate a new `boto3.Client` object to
        use in its operations. Alternatively, pass in an existing `boto3.Client`
        instance to re-use it here.

    raiseonfail : bool
        If True, will re-raise whatever Exception caused the operation to fail
        and break out immediately.

    Returns
    -------

    str
        Path to the downloaded filename or None if the download was
        unsuccessful.

    """

    if not client:
        client = client(keyfile, region=region, endpoint=endpoint)

    try:

        client.download_file(bucket, filename, local_file)
        return local_file

    except Exception as e:

        LOGEXCEPTION('could not download dos://%s/%s' % (bucket, filename))

        if raiseonfail:
            raise

        return None



def put_file(
        local_file,
        bucket,
        client=None,
        keyfile=None,
        region=None,
        endpoint=None,
        raiseonfail=False
):
    """This uploads a file to DOS.

    Parameters
    ----------

    local_file : str
        Path to the file to upload to DOS.

    bucket : str
        The DOS bucket to upload the file to.

    client : boto3.Client or None
        If None, this function will instantiate a new `boto3.Client` object to
        use in its operations. Alternatively, pass in an existing `boto3.Client`
        instance to re-use it here.

    raiseonfail : bool
        If True, will re-raise whatever Exception caused the operation to fail
        and break out immediately.

    Returns
    -------

    str or None
        If the file upload is successful, returns the dos:// URL of the uploaded
        file. If it failed, will return None.

    """

    if not client:
        client = client(keyfile, region=region, endpoint=endpoint)

    try:
        client.upload_file(local_file, bucket, os.path.basename(local_file))
        return 'dos://%s/%s' % (bucket, os.path.basename(local_file))
    except Exception as e:
        LOGEXCEPTION('could not upload %s to bucket: %s' % (local_file,
                                                            bucket))

        if raiseonfail:
            raise

        return None



def delete_file(
        bucket,
        filename,
        client=None,
        keyfile=None,
        region=None,
        endpoint=None,
        raiseonfail=False
):
    """This deletes a file from DOS.

    Parameters
    ----------

    bucket : str
        The AWS S3 bucket to delete the file from.

    filename : str
        The full file name of the file to delete, including any prefixes.

    client : boto3.Client or None
        If None, this function will instantiate a new `boto3.Client` object to
        use in its operations. Alternatively, pass in an existing `boto3.Client`
        instance to re-use it here.

    raiseonfail : bool
        If True, will re-raise whatever Exception caused the operation to fail
        and break out immediately.

    Returns
    -------

    bool
        If the file was successfully deleted, will return True.

    """

    if not client:
        client = client(keyfile, region=region, endpoint=endpoint)

    try:
        resp = client.delete_object(Bucket=bucket, Key=filename)
        if not resp:
            LOGERROR('could not delete file %s from bucket %s' % (filename,
                                                                  bucket))
        else:
            meta = resp.get('ResponseMetadata')
            return meta.get('HTTPStatusCode') == 204
    except Exception as e:
        LOGEXCEPTION('could not delete file %s from bucket %s' % (filename,
                                                                  bucket))
        if raiseonfail:
            raise

        return None
