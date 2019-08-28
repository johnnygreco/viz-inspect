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

except Exception:

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

from sqlalchemy import select, update, func, distinct
from sqlalchemy.dialects import postgresql as pg

import markdown
import bleach

from tqdm import tqdm

from .database import get_postgres_db, json_dumps


#########################
## LOADING THE CATALOG ##
#########################

DEFAULT_FLAGS = ('candy','galaxy', 'outskirts', 'junk', 'tidal', 'cirrus')


def load_catalog(catalog_fpath,
                 images_dpath,
                 dbinfo,
                 overwrite=False,
                 dbkwargs=None,
                 object_imagefile_pattern='hugs-{objectid}.png',
                 flags_to_use=DEFAULT_FLAGS,
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

        # this column now contains a JSON which tracks vote counts per flag
        x['user_flags'] = {x: 0 for x in flags_to_use}

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
        if (images_dpath.startswith('dos://') or
            images_dpath.startswith('s3://')):

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
    object_comments = meta.tables['object_comments']

    join = object_catalog.outerjoin(
        object_comments
    )

    sel = select(
        [object_catalog.c.id.label('keyid'),
         object_catalog.c.objectid,
         object_catalog.c.ra,
         object_catalog.c.dec,
         object_catalog.c.user_flags,
         object_catalog.c.extra_columns,
         object_catalog.c.review_status,
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
        userid_check=None,
        review_status='all',
        start_keyid=1,
        end_keyid=None,
        max_objects=50,
        getinfo='objectids',
        dbkwargs=None,
        fast_fetch=False,
        random_sample_percent=None,
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

    userid_check : tuple
        This is of the form: (userid_to_check, include_or_exclude) where
        userid_to_check is the integer user ID of the user to check and
        include_or_exclude is a string from {'include', 'exclude'}.

    review_status : str
        This is a string that indicates what kinds of objects to return.

        Choose from:

        - 'all' -> all objects
        - 'complete-good' -> objects that have at least 2 'good' votes
        - 'complete-bad' -> objects that have at least 2 'bad' votes
        - 'incomplete' -> objects that don't have at least 2 votes either way

    getinfo: {'objectids','all', 'count'}
        If 'objectids', returns only the objectids matching the specified
        criteria. If 'all', returns all info per object.

    dbkwargs : dict or None
        A dict of kwargs to pass to the database open function.

    fast_fetch : bool
        If True, returns bare tuples instead of nice dicts per row. This is much
        faster but not as convenient.

    random_sample_percent: float or None
        If this is provided, will be used to push the random sampling into the
        Postgres database itself. This must be a float between 0.0 and 100.0
        indicating the percentage of rows to sample.

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
    object_comments = meta.tables['object_comments']

    # add in the random sample if specified
    if random_sample_percent is not None:
        object_catalog_sample = object_catalog.tablesample(
            func.bernoulli(random_sample_percent)
        )
    else:
        object_catalog_sample = object_catalog

    join = object_catalog_sample.outerjoin(object_comments)

    if getinfo == 'all':

        sel = select([
            object_catalog_sample.c.id.label('keyid'),
            object_catalog_sample.c.objectid,
            object_catalog_sample.c.ra,
            object_catalog_sample.c.dec,
            object_catalog_sample.c.extra_columns,
            object_comments.c.added.label("comment_added_on"),
            object_comments.c.userid.label("comment_by_userid"),
            object_comments.c.username.label("comment_by_username"),
            object_comments.c.contents.label("comment_text"),
            object_comments.c.user_flags.label("comment_userset_flags"),
            object_catalog_sample.c.user_flags,
            object_catalog_sample.c.review_status
        ]).select_from(
            join
        )

    elif getinfo == 'count':

        sel = select(
            [func.count(distinct(object_catalog_sample.c.id))]
        ).select_from(join).distinct()

    else:
        sel = select([
            object_catalog_sample.c.id,
            object_catalog_sample.c.objectid,
        ]).select_from(join).distinct()

    #
    # get the actual selection on the review_status kwarg
    #

    if review_status == 'complete-good':
        actual_sel = sel.where(
            object_catalog_sample.c.review_status == 'complete-good'
        )
    elif review_status == 'complete-bad':
        actual_sel = sel.where(
            object_catalog_sample.c.review_status == 'complete-bad'
        )
    elif review_status == 'incomplete':
        actual_sel = sel.where(
            object_catalog_sample.c.review_status == 'incomplete'
        )
    else:
        actual_sel = sel

    #
    # add the user id handling
    #

    if userid_check is not None:

        userid_to_check, include_or_exclude = userid_check

        if include_or_exclude == 'include':
            actual_sel = actual_sel.where(
                object_comments.c.userid == userid_to_check
            )
        else:
            actual_sel = actual_sel.where(
                (object_comments.c.userid == None) |\
                (object_comments.c.userid != userid_to_check)
            )

    #
    # add in the pagination
    #

    # if only start_keyid is provided
    if (start_keyid is not None and
        end_keyid is None and
        max_objects is not None):
        paged_sel = actual_sel.where(
            object_catalog_sample.c.id >= start_keyid
        ).order_by(object_catalog_sample.c.id).limit(max_objects)
        revorder = False

    elif (start_keyid is not None and
          end_keyid is None and
          max_objects is None):
        paged_sel = actual_sel.where(
            object_catalog_sample.c.id >= start_keyid
        ).order_by(object_catalog_sample.c.id)
        revorder = False

    # if only end_keyid is provided
    elif (start_keyid is None and
          end_keyid is not None and
          max_objects is not None):
        paged_sel = actual_sel.where(
            object_catalog_sample.c.id <= end_keyid
        ).order_by(object_catalog_sample.c.id.desc()).limit(max_objects)
        revorder = True

    elif (start_keyid is None and
          end_keyid is not None and
          max_objects is None):
        paged_sel = actual_sel.where(
            object_catalog_sample.c.id <= end_keyid
        ).order_by(object_catalog_sample.c.id.desc())
        revorder = True

    # if both are provided
    elif (start_keyid is not None and
          end_keyid is not None):
        paged_sel = actual_sel.where(
            object_catalog_sample.c.id >= start_keyid
        ).where(
            object_catalog_sample.c.id <= end_keyid
        ).order_by(object_catalog_sample.c.id)
        revorder = False

    # everything else has no pagination
    else:
        paged_sel = actual_sel
        revorder = False

    with conn.begin():

        res = conn.execute(paged_sel)
        if fast_fetch:
            rows = res.fetchall()
            if len(rows) > 0:
                if not revorder:
                    ret_start_keyid = rows[0][0]
                    ret_end_keyid = rows[-1][0]
                else:
                    ret_start_keyid = rows[-1][0]
                    ret_end_keyid = rows[0][0]

            else:
                ret_start_keyid = 1
                ret_end_keyid = 1
        else:
            rows = [
                {column: value for column, value in row.items()} for row in res
            ]
            if len(rows) > 0:
                if not revorder:
                    ret_start_keyid = rows[0]['keyid']
                    ret_end_keyid = rows[-1]['keyid']
                else:
                    ret_start_keyid = rows[-1]['keyid']
                    ret_end_keyid = rows[0]['keyid']

            else:
                ret_start_keyid = 1
                ret_end_keyid = 1

    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()

    return rows, ret_start_keyid, ret_end_keyid, revorder


def get_object_count(
        dbinfo,
        userid_check=None,
        review_status='all',
        dbkwargs=None
):
    '''
    This counts all objects in the database.

    '''

    rows, start_keyid, end_keyid, revorder = get_objects(
        dbinfo,
        userid_check=userid_check,
        review_status=review_status,
        start_keyid=None,
        end_keyid=None,
        max_objects=None,
        getinfo='count',
        fast_fetch=True,
        dbkwargs=dbkwargs,
    )

    return rows[0][0]


def export_all_objects(outfile,
                       dbinfo,
                       dbkwargs=None):

    '''
    This exports all of the objects to a CSV.

    '''


######################
## UPDATING OBJECTS ##
######################

def insert_object_comments(
        userid,
        comments,
        dbinfo,
        good_flags,
        max_good_votes,
        bad_flags,
        max_bad_votes,
        max_all_votes,
        username=None,
        dbkwargs=None
):
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
    object_catalog = meta.tables['object_catalog']

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

        # check if this user has already commented on this object
        sel = select([object_comments.c.userid]).select_from(
            object_comments
        ).where(
            object_comments.c.objectid == objectid
        ).where(
            object_comments.c.userid == userid
        )

        res = conn.execute(sel)
        checkcount = res.rowcount
        res.close()

        if checkcount > 0:

            LOGINFO("Userid: %s has already commented on object: %s" %
                    (userid, objectid))
            updated = 0

        else:

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
            res = conn.execute(insert)
            updated = res.rowcount
            res.close()

            #
            # now update the counts for the object_flags
            #
            sel = select([object_catalog.c.user_flags]).where(
                object_catalog.c.objectid == objectid
            ).select_from(object_catalog)

            res = conn.execute(sel)
            flag_counts = res.scalar()

            for k in user_flags:
                if user_flags[k] is True:
                    flag_counts[k] = flag_counts[k] + 1

            # if any of the good/bad flags make it over the limits, set the
            # appropriate review_status
            bad_flag_sum = sum(flag_counts[k] for k in bad_flags)
            good_flag_sum = sum(flag_counts[k] for k in good_flags)
            all_flag_sum = bad_flag_sum + good_flag_sum

            if good_flag_sum >= max_good_votes:
                new_review_status = 'complete-good'
            elif bad_flag_sum >= max_bad_votes:
                new_review_status = 'complete-bad'
            elif (all_flag_sum < max_all_votes):
                new_review_status = 'incomplete'
            else:
                new_review_status = 'incomplete'

            # update the flags and review status
            upd = update(object_catalog).where(
                object_catalog.c.objectid == objectid
            ).values(
                {'user_flags':flag_counts,
                 'review_status':new_review_status}
            )

            res = conn.execute(upd)
            res.close()

    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()

    return updated


def update_vote(
        userid,
        dbinfo,
        objectid,
        new_vote,
        good_flags=['candy', 'galaxy'],
        bad_flags=['cirrus', 'junk', 'outskirts', 'tidal'],
        max_good_votes=2,
        max_bad_votes=2,
        max_all_votes=3,
        username=None,
        dbkwargs=None, 
        is_admin=False, 
        extra_comments=None,
):

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

    object_comments = meta.tables['object_comments']
    object_catalog = meta.tables['object_catalog']

    with conn.begin():

        added = updated = datetime.now(tz=utc)

        sel = select([object_comments.c.user_flags]).select_from(
            object_comments
        ).where(
            object_comments.c.objectid == objectid
        ).where(
            object_comments.c.userid == userid
        )

        res = conn.execute(sel)
        checkcount = res.rowcount
        this_user_flags = res.scalar()
        res.close()

        if (checkcount > 0) and not is_admin:
            _flag = [k for k, v in this_user_flags.items() if v]
            if len(_flag) != 1:
                raise Exception('Should be one vote!')
            this_user_flags[_flag[0]] = False
            this_user_flags[new_vote] = True

            LOGINFO('Changing the vote of user {} for hugs-{} from {} to {}'.\
                    format(userid, objectid, _flag[0], new_vote))

            sel = select([object_catalog.c.user_flags]).where(
                object_catalog.c.objectid == objectid
            ).select_from(object_catalog)

            res = conn.execute(sel)
            flag_counts = res.scalar()

            # update flag counts for new vote
            LOGINFO('Old flags: {}'.format(flag_counts))
            flag_counts[_flag[0]] -= 1
            flag_counts[new_vote] += 1
            LOGINFO('New flags: {}'.format(flag_counts))
            
            # if any of the good/bad flags make it over the limits, set the
            # appropriate review_status
            bad_flag_sum = sum(flag_counts[k] for k in bad_flags)
            good_flag_sum = sum(flag_counts[k] for k in good_flags)
            all_flag_sum = bad_flag_sum + good_flag_sum

            if good_flag_sum >= max_good_votes:
                new_review_status = 'complete-good'
            elif bad_flag_sum >= max_bad_votes:
                new_review_status = 'complete-bad'
            elif (all_flag_sum < max_all_votes):
                new_review_status = 'incomplete'
            else:
                new_review_status = 'incomplete'
        
            LOGINFO('Review status is: ' + new_review_status)

            # update the flags and review status
            upd = update(object_catalog).where(
                object_catalog.c.objectid == objectid
            ).values(
                {'user_flags':flag_counts,
                 'review_status':new_review_status}
            )

            res = conn.execute(upd)
            res.close()

            upd = update(object_comments).where(
                object_comments.c.objectid== objectid
            ).where(
                object_comments.c.userid == userid
            ).values({'user_flags':this_user_flags})

            res = conn.execute(upd)
            res.close()

        else:

            if not is_admin:
                LOGINFO('Inserting vote for user {} for hugs-{}'.\
                        format(userid, objectid))

            all_flags = good_flags + bad_flags

            # 1. bleach the comment
            _comment = 'ADMIN override'
            if extra_comments is not None:
                _comment = '{}, {}'.format(
                    extra_comments.replace('"', ''), _comment)
            cleaned_comment = bleach.clean(_comment, strip=True)

            # 2. markdown render the comment
            rendered_comment = markdown.markdown(
                cleaned_comment,
                output_format='html5',
            )

            user_flags = {f:False for f in all_flags}
            user_flags[new_vote] = True

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
            res = conn.execute(insert)
            updated = res.rowcount
            res.close()

            #
            # now update the counts for the object_flags
            #
            sel = select([object_catalog.c.user_flags]).where(
                object_catalog.c.objectid == objectid
            ).select_from(object_catalog)

            res = conn.execute(sel)
            flag_counts = res.scalar()

            if is_admin:
                LOGINFO('Admin overriding final vote for {} to {}'.\
                    format(objectid, new_vote))
                for k in user_flags:
                    flag_counts[k] = 0
                flag_counts[new_vote] = 2
            else:
                for k in user_flags:
                    if user_flags[k] is True:
                        flag_counts[k] = flag_counts[k] + 1

            # if any of the good/bad flags make it over the limits, set the
            # appropriate review_status
            bad_flag_sum = sum(flag_counts[k] for k in bad_flags)
            good_flag_sum = sum(flag_counts[k] for k in good_flags)
            all_flag_sum = bad_flag_sum + good_flag_sum

            if good_flag_sum >= max_good_votes:
                new_review_status = 'complete-good'
            elif bad_flag_sum >= max_bad_votes:
                new_review_status = 'complete-bad'
            elif (all_flag_sum < max_all_votes):
                new_review_status = 'incomplete'
            else:
                new_review_status = 'incomplete'

            # update the flags and review status
            upd = update(object_catalog).where(
                object_catalog.c.objectid == objectid
            ).values(
                {'user_flags':flag_counts,
                 'review_status':new_review_status}
            )

            res = conn.execute(upd)
            res.close()

    # close everything down if we were passed a database URL only and had to
    # make a new engine
    if engine:
        conn.close()
        meta.bind = None
        engine.dispose()
