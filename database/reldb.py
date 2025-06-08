#!/usr/bin/env python3

"""
Base file for the operational database.
"""


from sqlalchemy import create_engine
import constants
from model.reldb import metadata

engine = create_engine(constants.DATABASE_URL)
metadata = metadata