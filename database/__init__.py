#!/usr/bin/env python3

"""
This file contains database utilities.
"""

import sqlalchemy
import sqlalchemy.exc
from sqlalchemy.orm import sessionmaker

def ensure_schema(engine: sqlalchemy.Engine, metadata: sqlalchemy.MetaData):
    schema_name = metadata.schema
    if schema_name is None:
        raise ValueError("Schema name is None")
    with engine.begin() as conn:
        try:
            if not conn.dialect.has_schema(conn, schema_name):
                conn.execute(sqlalchemy.schema.CreateSchema(schema_name))
        except sqlalchemy.exc.ProgrammingError as e:
            if 'already exists' not in str(e):
                raise
    metadata.create_all(engine)
    print("+++++ Schema created successfully.")

def wipe_schema(engine: sqlalchemy.Engine, metadata: sqlalchemy.MetaData):
    schema_name = metadata.schema
    if schema_name is None:
        raise ValueError("Schema name is None")
    with engine.begin() as conn:
        try:
            if conn.dialect.has_schema(conn, schema_name):
                conn.execute(sqlalchemy.schema.DropSchema(schema_name, cascade=True))
        except sqlalchemy.exc.ProgrammingError as e:
            if 'does not exist' not in str(e):
                raise
    print("+++++ Schema wiped successfully.")

def truncate_schema(engine: sqlalchemy.Engine, metadata: sqlalchemy.MetaData):
    schema_name = metadata.schema
    if schema_name is None:
        raise ValueError("Schema name is None")
    with engine.begin() as conn:
        try:
            for table in metadata.tables.values():
                conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {schema_name}.{table.name} CASCADE"))
        except sqlalchemy.exc.ProgrammingError as e:
            if 'does not exist' not in str(e):
                raise 
    print("+++++ Schema truncated successfully.")

def get_session(engine: sqlalchemy.Engine):
    return sessionmaker(bind=engine)()