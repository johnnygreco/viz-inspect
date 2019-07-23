#!/usr/bin/env python
# -*- coding: utf-8 -*-
# authdb.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
# License: MIT - see the LICENSE file for the full text.

'''
This contains SQLAlchemy models for the authnzerver.

'''

from datetime import datetime

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy import (
    Table,
    Column,
    Integer,
    Float,
    Text,
    ForeignKey,
    MetaData
)

from sqlalchemy.dialects import postgresql
from sqlalchemy.types import TIMESTAMP


############
## TABLES ##
############

VIZINSPECT = MetaData()

Catalog = Table(
    'object_catalog',
    VIZINSPECT,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('objectid', Integer, nullable=False, index=True, unique=True),
    Column('added', TIMESTAMP(timezone=True),
           nullable=False,
           default=datetime.utcnow()),
    Column('updated', TIMESTAMP(timezone=True),
           nullable=False,
           onupdate=datetime.utcnow(),
           default=datetime.utcnow()),
    Column('ra', Float, nullable=False),
    Column('dec', Float, nullable=False),
    Column('user_flags', postgresql.JSONB),
#    Column('reviewer_userid', Integer, index=True),
    Column('extra_columns', postgresql.JSONB),
)

Catalog = Table(
    'object_images',
    VIZINSPECT,
    Column('imageid', Integer, primary_key=True, nullable=False),
    Column('objectid', Integer, ForeignKey('object_catalog.objectid'),
           nullable=False, index=True, unique=True),
    Column('added', TIMESTAMP(timezone=True),
           nullable=False, index=True,
           default=datetime.utcnow()),
    Column('updated', TIMESTAMP(timezone=True),
           nullable=False, index=True,
           onupdate=datetime.utcnow(),
           default=datetime.utcnow()),
    Column('filepath', Text, index=True, nullable=True),
)


Comments = Table(
    'object_comments',
    VIZINSPECT,
    Column('commentid', Integer, primary_key=True, nullable=False),
    Column('added', TIMESTAMP(timezone=True),
           nullable=False, index=True,
           default=datetime.utcnow()),
    Column('updated', TIMESTAMP(timezone=True),
           nullable=False, index=True,
           onupdate=datetime.utcnow(),
           default=datetime.utcnow()),
    Column('objectid', Integer, ForeignKey('object_catalog.objectid'),
           nullable=False, index=True),
    Column('userid', Integer, nullable=False, index=True),
    Column('reviewer_1', Integer, default=-99, nullable=False),
    Column('reviewer_2', Integer, default=-99, nullable=False),
    Column('reviewer_3', Integer, default=-99, nullable=False),
    Column('score', Integer, default=0, nullable=False),
    Column('num_votes', Integer, default=0, nullable=False),
    Column('username', Text, index=True),
    # this is the per-user set flags
    Column('user_flags', postgresql.JSONB),
    Column('contents', Text),
)


#######################################
## JSON SERIALIZERS AND DESERIALIZERS ##
########################################

# we need this to send objects with the following types to the frontend:
# - bytes
# - ndarray
# - datetime
import json
import numpy as np
from sqlalchemy.engine.result import RowProxy


class DatabaseJSONEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode()
        elif isinstance(obj, complex):
            return (obj.real, obj.imag)
        elif (isinstance(obj, (float, np.float32, np.float64, np.float_)) and
              not np.isfinite(obj)):
            return None
        elif isinstance(obj, (np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, RowProxy):
            return tuple(obj)
        else:
            return json.JSONEncoder.default(self, obj)

def json_dumps(obj):
    '''
    This uses a customized JSONEncoder to be able to serialize more things.

    '''

    dumped = json.dumps(obj, cls=DatabaseJSONEncoder)
    dumped = dumped.replace('NaN','null')
    return dumped


#######################
## DATABASE CREATION ##
#######################

def new_vizinspect_db(
        database_url,
        database_metadata,
        echo=False,
        returnconn=False
):
    '''This makes a new vizinspect PostgreSQL database.

    Parameters
    ----------

    database_url : str
        A valid SQLAlchemy database connection string.

    echo : bool
        If True, will echo the DDL lines used for creation of the database.

    returnconn : bool
        If True, will return the engine and the metadata object after creating
        the database.

    Returns
    -------

    tuple or None
        If `returnconn` is True, will return `(engine, metadata)`. If it is
        False, will return None.

    '''

    engine = create_engine(database_url, echo=echo)
    database_metadata.create_all(engine)

    if returnconn:
        return engine, database_metadata
    else:
        engine.dispose()
        del engine

    # if the engine is SQLite, switch it to WAL mode
    if 'sqlite' in database_url:

        import sqlite3
        from textwrap import dedent

        WAL_MODE_SCRIPT = dedent(
            '''\
            pragma journal_mode = 'wal';
            pragma journal_size_limit = 5242880;
            '''
        )

        db_path = database_url.replace('sqlite:///','')
        db = sqlite3.connect(db_path)
        cur = db.cursor()
        cur.executescript(WAL_MODE_SCRIPT)
        db.commit()
        db.close()



#########################
## DATABASE CONNECTION ##
#########################

def get_vizinspect_db(database_url,
                      database_metadata,
                      use_engine=None,
                      engine_dispose=False,
                      engine_kwargs=None,
                      echo=False):
    '''This returns a database connection to use for queries.

    Parameters
    ----------

    database_url : str
        A valid SQLAlchemy database connection string.

    database_metadata : sqlalchemy.MetaData object
        The metadata object to bind to the engine.

    use_engine : `sqlalchemy.engine.Engine` object or None
        If provided, will use this existing engine object to get a connection.

    engine_dispose : bool
        If True, will run the `Engine.dispose()` method before binding to
        it. This can help get rid any existing connections.

    engine_kwargs : dict or None
        This contains any kwargs to pass to the `create_engine` call. One
        specific use-case is passing `use_batch_mode=True` to a PostgreSQL
        engine to enable fast `executemany` statements.

    echo : bool
        If True, will echo the DDL lines used for creation of the database.

    Returns
    -------

    (engine, connection, metadata) : tuple
        This function will return the engine, the DB connection generated, and
        the metadata object as a tuple.

    '''

    # handle foreign key constraint enforcement on SQLite
    if 'sqlite' in database_url:

        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()


    if not use_engine:
        if isinstance(engine_kwargs, dict):
            database_engine = create_engine(database_url,
                                            echo=echo,
                                            **engine_kwargs)
        else:
            database_engine = create_engine(database_url,
                                            echo=echo)

    else:
        database_engine = use_engine
        if engine_dispose:
            database_engine.dispose()

    database_metadata.bind = database_engine
    conn = database_engine.connect()

    return database_engine, conn, database_metadata



def get_postgres_db(database_url,
                    database_metadata,
                    use_engine=None,
                    engine_dispose=False,
                    engine_kwargs=None,
                    echo=False):
    '''This gets the postgres DB.

    Parameters
    ----------

    database_url : str
        A valid SQLAlchemy database connection string.

    database_metadata : sqlalchemy.MetaData object
        The metadata object to bind to the engine.

    use_engine : `sqlalchemy.engine.Engine` object or None
        If provided, will use this existing engine object to get a connection.

    engine_dispose : bool
        If True, will run the `Engine.dispose()` method before binding to
        it. This can help get rid any existing connections.

    engine_kwargs : dict or None
        This contains any kwargs to pass to the `create_engine` call. One
        specific use-case is passing `use_batch_mode=True` to a PostgreSQL
        engine to enable fast `executemany` statements.

    echo : bool
        If True, will echo the DDL lines used for creation of the database.

    Returns
    -------

    (engine, connection, metadata) : tuple
        This function will return the engine, the DB connection generated, and
        the metadata object as a tuple.

    '''

    if engine_kwargs is None:
        engine_kwargs = {'use_batch_mode':True}
    elif isinstance(engine_kwargs, dict):
        engine_kwargs.update({'use_batch_mode':True})

    return get_vizinspect_db(database_url,
                             database_metadata,
                             use_engine=use_engine,
                             engine_dispose=engine_dispose,
                             engine_kwargs=engine_kwargs,
                             echo=echo)
