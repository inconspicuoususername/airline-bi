

from etl.star_schema import incremental_load_star_schema
from etl.warehouse import incremental_load_warehouse
import database
import database.warehouse as whdb
import database.star_schema as star_db

import constants

database.ensure_schema(whdb.engine,whdb.metadata)
database.ensure_schema(star_db.engine,star_db.metadata)

incremental_load_warehouse(5)
incremental_load_star_schema()