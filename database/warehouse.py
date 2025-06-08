#!/usr/bin/env python3

"""
Base file for the warehouse database.
"""

from sqlalchemy import create_engine
import constants
from model.warehouse import metadata

engine = create_engine(constants.DATABASE_URL)
metadata = metadata