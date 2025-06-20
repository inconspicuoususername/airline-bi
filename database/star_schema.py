#!/usr/bin/env python3

"""
Base file for the star schema database.
"""

from sqlalchemy import create_engine, MetaData
import constants
import model.star_schema as star_schema

engine = create_engine(constants.DATABASE_URL)
metadata = star_schema.metadata