from sqlalchemy import create_engine
import constants
from model.warehouse import metadata

engine = create_engine(constants.DATABASE_URL)
metadata = metadata