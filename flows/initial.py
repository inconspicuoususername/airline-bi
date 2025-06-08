import prefect
from etl.warehouse import full_load_warehouse_2, incremental_load_warehouse
from etl.star_schema import full_load_star_schema, incremental_load_star_schema
from notifications import slack
from util.batch_id import get_batch_id, set_batch_id

@prefect.task(name="initial-warehouse-load")
def initial_load(
    batch_id: int,
):
    print("Starting initial load")

    try:
        slack.send_message("Airline ETL: Starting initial load into warehouse")
        full_load_star_schema(batch_id)
        slack.send_message("Airline ETL: Warehouse loaded successfully!")
        slack.send_message("Airline ETL: Starting initial load into star schema")
        full_load_warehouse_2(batch_id)
        slack.send_message("Airline ETL: Star schema loaded successfully!")
    except Exception as e:
        print(e)
        slack.send_message("Airline ETL: Initial load failed!\nError: " + str(e))

    slack.send_message("Airline ETL: Initial load completed successfully!")
@prefect.task(name="incremental-warehouse-load")
def incremental_load(
    batch_id: int,
):
    try:
        slack.send_message("Airline ETL: Starting incremental load into warehouse")
        incremental_load_warehouse(batch_id)
        slack.send_message("Airline ETL: Warehouse loaded successfully!")
        slack.send_message("Airline ETL: Starting incremental load into star schema")
        incremental_load_star_schema()
        slack.send_message("Airline ETL: Star schema loaded successfully!")
    except Exception as e:
        print(e)
        slack.send_message("Airline ETL: Incremental load failed!\nError: " + str(e))

@prefect.flow(name="airline-etl", retries=3, retry_delay_seconds=120)
def airline_etl():
    try:
        batch_id = get_batch_id()
        incremental_load(batch_id)
    except Exception as e:
        print(e)

    set_batch_id(batch_id + 1)


if __name__ == "__main__":
    # -------------------------------------------------------
    # Launch Prefect’s local agent:
    #   • Enqueue a new run every 5 minutes (300 seconds)
    #   • Runs in this process until CTRL+C
    # -------------------------------------------------------
    airline_etl.serve(
        name="incremental-load",
        interval=300,
        tags=["bi_project"],
        pause_on_shutdown=False,
    )
    