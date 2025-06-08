

from data import synthesize_reldb
from etl.star_schema import full_load_star_schema, incremental_load_star_schema
from etl.warehouse import full_load_warehouse_2, incremental_load_warehouse
import database
import database.warehouse as whdb
import database.star_schema as star_db

import constants

synthesize_reldb.synthesize_reldb()

full_load_warehouse_2(1)
full_load_star_schema(1)

incremental_load_warehouse(2)
incremental_load_star_schema()