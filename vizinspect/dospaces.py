#!/usr/bin/env python
# -*- coding: utf-8 -*-
# bucketstorage.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Apr 2019
# License: MIT - see the LICENSE file for the full text.

"""This contains functions that handle AWS S3/Digital Ocean
Spaces/S3-compatible bucket operations.

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

def client(
        keyfile,
        region='sfo2',
        endpoint='https://sfo2.digitaloceanspaces.com'
):
    '''This makes a new bucket client.

    Requires a keyfile containing the access token and the secret key in the
    following format::

        access_token secret_key

    The default `region` and `endpoint` assume you're using Digital Ocean
    Spaces.

    If you're using S3, see:
    https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region to figure
    out the values for `region` and `endpoint`.

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

    """This gets a file from abucket.

    Parameters
    ----------

    bucket : str
        The bucket name.

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

        LOGEXCEPTION('could not download %s from bucket: %s' %
                     (filename, bucket))

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
    """This uploads a file to a bucket.

    Parameters
    ----------

    local_file : str
        Path to the file to upload to bucket.

    bucket : str
        The bucket to upload the file to.

    client : boto3.Client or None
        If None, this function will instantiate a new `boto3.Client` object to
        use in its operations. Alternatively, pass in an existing `boto3.Client`
        instance to re-use it here.

    raiseonfail : bool
        If True, will re-raise whatever Exception caused the operation to fail
        and break out immediately.

    Returns
    -------

    True
        If the file upload is successful, returns True

    """

    if not client:
        client = client(keyfile, region=region, endpoint=endpoint)

    try:
        client.upload_file(local_file, bucket, os.path.basename(local_file))
        return True
    except Exception as e:
        LOGEXCEPTION('could not upload %s to bucket: %s' % (local_file,
                                                            bucket))

        if raiseonfail:
            raise

        return False



def delete_file(
        bucket,
        filename,
        client=None,
        keyfile=None,
        region=None,
        endpoint=None,
        raiseonfail=False
):
    """This deletes a file from a bucket.

    Parameters
    ----------

    bucket : str
        The bucket to delete the file from.

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
