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

try:

    from datetime import datetime, timezone, timedelta
    utc = timezone.utc

except Exception as e:

    from datetime import datetime, timedelta, tzinfo
    ZERO = timedelta(0)

    class UTC(tzinfo):
        """UTC"""

        def utcoffset(self, dt):
            return ZERO

        def tzname(self, dt):
            return "UTC"

        def dst(self, dt):
            return ZERO

    utc = UTC()

from sqlalchemy import select, update
from sqlalchemy.dialects import postgresql as pg

import markdown
import bleach

from tqdm import tqdm

from .database import get_postgres_db, json_dumps


#########################
## LOADING THE CATALOG ##
#########################

def load_catalog(catalog_fpath,
                 images_dpath,
                 dbinfo,
                 overwrite=False,
                 dbkwargs=None,
                 object_imagefile_pattern='hugs-{objectid}.png',
                 flags_to_use=('candy','junk','tidal','cirrus'),
                 **pdkwargs):
    '''This loads the catalog into the vizinspect database object_catalog table.

    Parameters
    ----------

    catalog_fpath : str
        The path to the CSV to load.

    images_dpath : str
        The path to the images directory. If this starts with 'dos://', this
        function will assume that the images are in a Digital Ocean Space. In
        this case, the images_dpath should include the bucket name and be of the
        following form::

            'dos://<bucket-name>'

    dbinfo : tuple
        This is a tuple of two items:

        - the database URL or the connection instance to use
        - the database metadata object

        If the database URL is provided, a new engine will be used. If the
        connection itself is provided, it will be re-used.

    overwrite : bool
        If True, will overwrite existing objects with the same objectid.

    dbkwargs : dict or None
        A dict of kwargs to pass to the database open function.

    object_imagefile_pattern : str
        A template for the filename corresponding to the HUGS image for each
        object. This must have a '{objectid}' key somewhere in it; this will be
        filled in for each object and the image will be searched for in ther
        `images_dpath`.

    flags_to_use : sequence of str
        These are the flag column names to add into the catalog when loading it
        for the first time.

    pdkwargs : extra keyword arguments
        All of these will passed directly to the `pandas.read_csv` function.

    Returns
    -------

    bool
        True if all objects were loaded successfully into the database.

    '''

    # read the catalog
    catalog = pd.read_csv(catalog_fpath, **pdkwargs)

    #
    # get the database
    #
    dbref, dbmeta = dbinfo
    if not dbkwargs:
        dbkwargs = {}
    if isinstance(dbref, str) and 'postgres' in dbref:
        if not dbkwargs:
            dbkwargs = {'engine_kwargs':{'json_serializer':json_dumps}}
        elif dbkwargs and 'engine_kwargs' not in dbkwargs:
            dbkwargs['engine_kwargs'] = {'json_serializer':json_dumps}
        elif dbkwargs and 'engine_kwargs' in dbkwargs:
            dbkwargs['engine_kwargs'].update({'json_serializer':json_dumps})
        engine, conn, meta = get_postgres_db(dbref,
                                             dbmeta,
                                             **dbkwargs)
    elif isinstance(dbref, str) and 'postgres' not in dbref:
        raise NotImplementedError(
            "viz-inspect currently doesn't support non-Postgres databases."
        )
    else:
        engine, conn, meta = None, dbref, dbmeta
        meta.bind = conn
    #
    # end of database get
    #

    # get the columns out of the catalog
    column_list = list(catalog.columns)
    maincol_list = ['ra','dec']
    othercol_list = list(set(column_list) - set(maincol_list))

    # the main columns
    main_cols = catalog[maincol_list].to_dict(orient='records')
    other_cols = catalog[othercol_list].to_dict(orient='records')

    now = datetime.now(tz=utc)

    for x,y in zip(main_cols, other_cols):

        x['extra_columns'] = y
        x['added'] = now
        x['updated'] = now
        x['objectid'] = y['viz-id']
        x['user_flags'] = {x: False for x in flags_to_use}

    # get the table
    object_catalog = meta.tables['object_catalog']


    # execute the inserts
    with conn.begin():

        LOGINFO("Inserting object rows...")

        for row in tqdm(main_cols):

            # prepare the insert
            insert = pg.insert(
                object_catalog
            ).values(
                row
            )

            if overwrite:
                insert = insert.on_conflict_do_update(
                    index_elements=[object_catalog.c.objectid],
                    set_={'updated':now,
                          'user_flags':row['user_flags'],
                          'extra_columns':row['extra_columns'],
                          'ra':row['ra'],
                          'dec':row['dec']}
                )

            # insert the object rows
            conn.execute(insert)

        LOGINFO("Inserting object image file paths...")

        # here, if the images are all remote, then we don't check if they exist
        # locally. the server frontend will take care of getting them later.
        if images_dpath.startswith('dos://'):

            object_images = [
                '%s/%s' % (
                    images_dpath.rstrip('/'),
                    object_imagefile_pattern.format(
                        objectid=int(x['objectid'])
                    )
                ) for x in main_cols
            ]

        # otherwise, the images are in a local directory
        else:

            # look up the images for each object
            object_images = [
                os.path.join(
                    images_dpath,
                    object_imagefile_pattern.format(
                        objectid=int(x['objectid'])
                    )
                ) for x in main_cols
            ]

            object_images = [
                (os.path.abspath(x) if os.path.exists(x) else None)
                for x in object_images
            ]


        # generate the object insertion rows
        image_cols = [
            {'objectid':x,
             'added':now,
             'updated':now,
             'filepath':y}
            for x,y in zip([a['objectid'] for a in main_cols], object_images)
        ]

        # get the table
        object_images = meta.tables['object_images']

        for row in tqdm(image_cols):

            # prepare the insert
            insert = pg.insert(
                object_images
            ).values(
                row
            )

            if overwrite:
                insert = insert.on_conflict_do_update(
                    index_elements=[object_images.c.objectid],
                    set_={
                        'updated':now,
                        'filepath':row['filepath']
                    }
                )

            conn.execute(insert)


    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()

    # if we make it to here, the insert was successful
    LOGINFO('Inserted %s new objects.' % len(catalog))
    return True


########################################
## GETTING OBJECTS OUT OF THE CATALOG ##
########################################

def get_object(objectid,
               dbinfo,
               dbkwargs=None):
    '''
    This gets a single object out of the catalog.

    Parameters
    ----------

    objectid : int
        The object ID for the object to retrieve.

    dbinfo : tuple
        This is a tuple of two items:

        - the database URL or the connection instance to use
        - the database metadata object

        If the database URL is provided, a new engine will be used. If the
        connection itself is provided, it will be re-used.

    dbkwargs : dict or None
        A dict of kwargs to pass to the database open function.

    Returns
    -------

    dict
        Returns all of the object's info as a dict.

    '''

    #
    # get the database
    #
    dbref, dbmeta = dbinfo
    if not dbkwargs:
        dbkwargs = {}
    if isinstance(dbref, str) and 'postgres' in dbref:
        if not dbkwargs:
            dbkwargs = {'engine_kwargs':{'json_serializer':json_dumps}}
        elif dbkwargs and 'engine_kwargs' not in dbkwargs:
            dbkwargs['engine_kwargs'] = {'json_serializer':json_dumps}
        elif dbkwargs and 'engine_kwargs' in dbkwargs:
            dbkwargs['engine_kwargs'].update({'json_serializer':json_dumps})
        engine, conn, meta = get_postgres_db(dbref,
                                             dbmeta,
                                             **dbkwargs)
    elif isinstance(dbref, str) and 'postgres' not in dbref:
        raise NotImplementedError(
            "PIPE-TrEx currently doesn't support non-Postgres databases."
        )
    else:
        engine, conn, meta = None, dbref, dbmeta
        meta.bind = conn
    #
    # end of database get
    #

    # prepare the select
    object_catalog = meta.tables['object_catalog']
    object_images = meta.tables['object_images']
    object_comments = meta.tables['object_comments']

    join = object_catalog.join(object_images).outerjoin(object_comments)

    sel = select(
        [object_catalog.c.id.label('keyid'),
         object_catalog.c.objectid,
         object_catalog.c.ra,
         object_catalog.c.dec,
         object_catalog.c.user_flags,
         object_catalog.c.reviewer_userid,
         object_catalog.c.extra_columns,
         object_images.c.filepath,
         object_comments.c.added.label("comment_added_on"),
         object_comments.c.userid.label("comment_by_userid"),
         object_comments.c.username.label("comment_by_username"),
         object_comments.c.user_flags.label("comment_userset_flags"),
         object_comments.c.contents.label("comment_text")]
    ).select_from(
        join
    ).where(
        object_catalog.c.objectid == objectid
    ).order_by(
        object_comments.c.added.desc()
    )

    with conn.begin():

        res = conn.execute(sel)
        rows = [{column: value for column, value in row.items()} for row in res]

    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()

    return rows



def get_objects(
        dbinfo,
        review_status='all',
        userid=None,
        start_keyid=0,
        end_keyid=50,
        getinfo='objectids',
        dbkwargs=None
):
    '''This is used to get object lists filtering on either userids or review
    status or both.

    Parameters
    ----------

    dbinfo : tuple
        This is a tuple of two items:

        - the database URL or the connection instance to use
        - the database metadata object

        If the database URL is provided, a new engine will be used. If the
        connection itself is provided, it will be re-used.

    review_status : str
        This is a string that indicates what kinds of objects to return.

        Choose from:

        - 'all' -> all objects
        - 'reviewed-all' -> all objects that have been reviewed
        - 'unreviewed-all' -> all objects that have not been reviewed
        - 'reviewed-self' -> objects reviewed by this user
        - 'reviewed-other' -> objects reviewed by other users
        - 'assigned-self' -> objects assigned to this user
        - 'assigned-reviewed' -> objects assigned to self and reviewed
        - 'assigned-unreviewed' -> objects assigned to self but unreviewed
        - 'assigned-all' -> all objects assigned to some reviewer
        - 'unassigned-all' -> all objects not assigned to any reviewers

    userid : int
        If set, sets the current userid to use in the filters when review_status
        is one of the -self, -other values.

    getinfo: {'objectids','review-assignments','all'}
        If 'objectids', returns only the objectids matching the specified
        criteria. If 'review-assignments', returns the objectids and a list of
        userids assigned to review each object. If 'all', returns all info per
        object.

    dbkwargs : dict or None
        A dict of kwargs to pass to the database open function.

    Returns
    -------

    dict
        Returns all of the object's info as a dict.

    '''

    #
    # get the database
    #
    dbref, dbmeta = dbinfo
    if not dbkwargs:
        dbkwargs = {}
    if isinstance(dbref, str) and 'postgres' in dbref:
        if not dbkwargs:
            dbkwargs = {'engine_kwargs':{'json_serializer':json_dumps}}
        elif dbkwargs and 'engine_kwargs' not in dbkwargs:
            dbkwargs['engine_kwargs'] = {'json_serializer':json_dumps}
        elif dbkwargs and 'engine_kwargs' in dbkwargs:
            dbkwargs['engine_kwargs'].update({'json_serializer':json_dumps})
        engine, conn, meta = get_postgres_db(dbref,
                                             dbmeta,
                                             **dbkwargs)
    elif isinstance(dbref, str) and 'postgres' not in dbref:
        raise NotImplementedError(
            "viz-inspect currently doesn't support non-Postgres databases."
        )
    else:
        engine, conn, meta = None, dbref, dbmeta
        meta.bind = conn
    #
    # end of database get
    #

    # prepare the select
    object_catalog = meta.tables['object_catalog']
    object_images = meta.tables['object_images']
    object_comments = meta.tables['object_comments']

    join = object_catalog.join(object_images).outerjoin(object_comments)

    if getinfo == 'all':

        sel = select(
            [object_catalog.c.id.label('keyid'),
             object_catalog.c.objectid,
             object_catalog.c.ra,
             object_catalog.c.dec,
             object_catalog.c.user_flags,
             object_catalog.c.reviewer_userid,
             object_catalog.c.extra_columns,
             object_images.c.filepath,
             object_comments.c.added.label("comment_added_on"),
             object_comments.c.userid.label("comment_by_userid"),
             object_comments.c.username.label("comment_by_username"),
             object_comments.c.user_flags.label("comment_userset_flags"),
             object_comments.c.contents.label("comment_text")]
        ).select_from(
            join
        )

    elif getinfo == 'objectids':
        sel = select([object_catalog.c.objectid]).select_from(join)

    elif getinfo == 'review-assignments':
        sel = select([object_catalog.c.objectid,
                      object_catalog.c.reviewer_userid]).select_from(join)

    else:
        sel = select([object_catalog.c.objectid]).select_from(join)

    # figure out the where condition
    if review_status == 'unreviewed-all':
        actual_sel = sel.where(object_comments.c.added == None)
    elif review_status == 'reviewed-all':
        actual_sel = sel.where(object_comments.c.added != None)

    elif review_status == 'reviewed-self' and userid is not None:
        actual_sel = sel.where(
            object_comments.c.userid == userid
        ).where(
            object_comments.c.added != None
        )
    elif review_status == 'reviewed-other' and userid is not None:
        actual_sel = sel.where(
            object_comments.c.userid != userid
        ).where(
            object_comments.c.added != None
        )
    elif review_status == 'assigned-self' and userid is not None:
        actual_sel = sel.where(
            object_catalog.c.reviewer_userid == userid
        )
    elif review_status == 'assigned-reviewed' and userid is not None:
        actual_sel = sel.where(
            object_catalog.c.reviewer_userid == userid
        ).where(
            object_comments.c.added != None
        )
    elif review_status == 'assigned-unreviewed' and userid is not None:
        actual_sel = sel.where(
            object_catalog.c.reviewer_userid == userid
        ).where(
            object_comments.c.added == None
        )
    elif review_status == 'assigned-all':
        actual_sel = sel.where(
            object_catalog.c.reviewer_userid != None
        )
    elif review_status == 'unassigned-all':
        actual_sel = sel.where(
            object_catalog.c.reviewer_userid == None
        )
    else:
        actual_sel = sel

    # add in the pagination
    if start_keyid is not None and end_keyid is not None:
        paged_sel = actual_sel.where(
            object_catalog.c.id >= start_keyid
        ).where(
            object_catalog.c.id <= end_keyid
        )
    elif start_keyid is not None and end_keyid is None:
        paged_sel = actual_sel.where(
            object_catalog.c.id >= start_keyid
        )
    elif end_keyid is not None and start_keyid is None:
        paged_sel = actual_sel.where(
            object_catalog.c.id <= end_keyid
        )
    else:
        paged_sel = actual_sel


    with conn.begin():

        res = conn.execute(paged_sel)
        rows = [{column: value for column, value in row.items()} for row in res]

    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()

    return rows, start_keyid, end_keyid



def export_all_objects(outfile,
                       dbinfo,
                       dbkwargs=None):

    '''
    This exports all of the objects to a CSV.

    '''


######################
## UPDATING OBJECTS ##
######################

def insert_object_comments(userid,
                           comments,
                           dbinfo,
                           username=None,
                           dbkwargs=None):
    '''This inserts a comment for the object.

    Markdown is allowed in the comment text. The comment text will be run
    through bleach to remove harmful bits.

    Parameters
    ----------

    userid : int
        The userid of the user making the comment.

    comments : dict
        The content of the comments from the frontend. This is a dict of the
        form::

            {"objectid": int, the object ID for which the comments are intended,
             "comment": str, the comment text,
             "user_flags": dict, the flags set by the user and their value}

    dbinfo : tuple
        This is a tuple of two items:

        - the database URL or the connection instance to use
        - the database metadata object

        If the database URL is provided, a new engine will be used. If the
        connection itself is provided, it will be re-used.

    username : str or None
        The name of the user making the comment.

    dbkwargs : dict or None
        A dict of kwargs to pass to the database open function.

    Returns
    -------

    dict
        Returns all of the object's info as a dict.

    '''

    #
    # get the database
    #
    dbref, dbmeta = dbinfo
    if not dbkwargs:
        dbkwargs = {}
    if isinstance(dbref, str) and 'postgres' in dbref:
        if not dbkwargs:
            dbkwargs = {'engine_kwargs':{'json_serializer':json_dumps}}
        elif dbkwargs and 'engine_kwargs' not in dbkwargs:
            dbkwargs['engine_kwargs'] = {'json_serializer':json_dumps}
        elif dbkwargs and 'engine_kwargs' in dbkwargs:
            dbkwargs['engine_kwargs'].update({'json_serializer':json_dumps})
        engine, conn, meta = get_postgres_db(dbref,
                                             dbmeta,
                                             **dbkwargs)
    elif isinstance(dbref, str) and 'postgres' not in dbref:
        raise NotImplementedError(
            "viz-inspect currently doesn't support non-Postgres databases."
        )
    else:
        engine, conn, meta = None, dbref, dbmeta
        meta.bind = conn
    #
    # end of database get
    #

    # prepare the tables
    object_comments = meta.tables['object_comments']

    with conn.begin():
        added = updated = datetime.now(tz=utc)

        objectid = comments['objectid']
        comment_text = comments['comment']
        user_flags = comments['user_flags']

        # 1. bleach the comment
        cleaned_comment = bleach.clean(comment_text, strip=True)

        # 2. markdown render the comment
        rendered_comment = markdown.markdown(
            cleaned_comment,
            output_format='html5',
        )

        # prepare the insert
        insert = pg.insert(
            object_comments
        ).values(
            {'objectid':objectid,
             'added':added,
             'updated':updated,
             'userid':userid,
             'username':username,
             'user_flags':user_flags,
             'contents':rendered_comment}
        )
        conn.execute(insert)

    # now look up the object and get all of its info
    with conn.begin():
        objectinfo = get_object(objectid, (conn, meta), dbkwargs)

    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()

    return objectinfo



def update_object_flags(objectid,
                        flags,
                        dbinfo,
                        dbkwargs=None):
    '''
    This updates an object's flags.

    Parameters
    ----------

    objectid : int
        The objectid of the object being updated.

    flags : dict
        The flags from the frontend as a dict with keys = the names of the flags
        and the values as booleans representing if the flag is set or None.

    dbinfo : tuple
        This is a tuple of two items:

        - the database URL or the connection instance to use
        - the database metadata object

        If the database URL is provided, a new engine will be used. If the
        connection itself is provided, it will be re-used.

    dbkwargs : dict or None
        A dict of kwargs to pass to the database open function.

    Returns
    -------

    dict
        Returns all of the object's info as a dict.

    '''

    #
    # get the database
    #
    dbref, dbmeta = dbinfo
    if not dbkwargs:
        dbkwargs = {}
    if isinstance(dbref, str) and 'postgres' in dbref:
        if not dbkwargs:
            dbkwargs = {'engine_kwargs':{'json_serializer':json_dumps}}
        elif dbkwargs and 'engine_kwargs' not in dbkwargs:
            dbkwargs['engine_kwargs'] = {'json_serializer':json_dumps}
        elif dbkwargs and 'engine_kwargs' in dbkwargs:
            dbkwargs['engine_kwargs'].update({'json_serializer':json_dumps})
        engine, conn, meta = get_postgres_db(dbref,
                                             dbmeta,
                                             **dbkwargs)
    elif isinstance(dbref, str) and 'postgres' not in dbref:
        raise NotImplementedError(
            "viz-inspect currently doesn't support non-Postgres databases."
        )
    else:
        engine, conn, meta = None, dbref, dbmeta
        meta.bind = conn
    #
    # end of database get
    #

    # prepare the tables
    object_catalog = meta.tables['object_catalog']

    upd = update(object_catalog).where(
        object_catalog.c.objectid == objectid
    ).values(
        {'user_flags':flags}
    )

    with conn.begin():
        conn.execute(upd)

    # now look up the object and get all of its info
    with conn.begin():
        objectinfo = get_object(objectid, (conn, meta), dbkwargs)

    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()

    return objectinfo


#######################
## ASSIGNING OBJECTS ##
#######################

def update_review_assignments(objectid_list,
                              reviewer_userid,
                              dbinfo,
                              do_unassign=False,
                              dbkwargs=None):
    '''
    This updates review assignments for a list of objectids


    Parameters
    ----------

    objectid_list : list of ints
        The objectids of the object being updated.

    reviewer_userid : int
        The userid of the reviewer.

    do_unassign : bool
        If False, will assign the objects in `objectid_list` to
        `reviewer_userid`. If True, will unassign objects in `objectid_list`
        from the reviewer, i.e. set their `reviewer_userid` to NULL.

    dbinfo : tuple
        This is a tuple of two items:

        - the database URL or the connection instance to use
        - the database metadata object

        If the database URL is provided, a new engine will be used. If the
        connection itself is provided, it will be re-used.

    dbkwargs : dict or None
        A dict of kwargs to pass to the database open function.

    Returns
    -------

    dict
        Returns all of the object's info as a dict.

    '''

    #
    # get the database
    #
    dbref, dbmeta = dbinfo
    if not dbkwargs:
        dbkwargs = {}
    if isinstance(dbref, str) and 'postgres' in dbref:
        if not dbkwargs:
            dbkwargs = {'engine_kwargs':{'json_serializer':json_dumps}}
        elif dbkwargs and 'engine_kwargs' not in dbkwargs:
            dbkwargs['engine_kwargs'] = {'json_serializer':json_dumps}
        elif dbkwargs and 'engine_kwargs' in dbkwargs:
            dbkwargs['engine_kwargs'].update({'json_serializer':json_dumps})
        engine, conn, meta = get_postgres_db(dbref,
                                             dbmeta,
                                             **dbkwargs)
    elif isinstance(dbref, str) and 'postgres' not in dbref:
        raise NotImplementedError(
            "viz-inspect currently doesn't support non-Postgres databases."
        )
    else:
        engine, conn, meta = None, dbref, dbmeta
        meta.bind = conn
    #
    # end of database get
    #

    # prepare the tables
    object_catalog = meta.tables['object_catalog']

    if not do_unassign:

        upd = update(object_catalog).where(
            object_catalog.c.objectid.in_(list(objectid_list))
        ).values(
            {'reviewer_userid':reviewer_userid}
        )

    else:

        upd = update(object_catalog).where(
            object_catalog.c.objectid.in_(list(objectid_list))
        ).values(
            {'reviewer_userid':None}
        )

    with conn.begin():
        res = conn.execute(upd)
        retval = res.rowcount
        res.close()

    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()

    return retval
