

from data.synthesize_reldb import synthesize_reldb
from etl.star_schema import full_load_star_schema, incremental_load_star_schema
from etl.warehouse import full_load_warehouse_2, incremental_load_csv_staging, incremental_load_warehouse
import database
import database.warehouse as whdb
import database.star_schema as star_db

import constants


#NOTE: Running synthesize_reldb from index.py causes a bunch of zombie processes or threads to be created
#therefore, if you want to synthesize the data, run it as `python3 -m data.synthesize_reldb``
# synthesize_reldb()

full_load_warehouse_2(1)

# incremental_load_csv_staging(1, "data/output/reviews.csv")
full_load_star_schema(1)

# incremental_load_warehouse(2)
# incremental_load_star_schema()