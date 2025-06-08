import prefect
from etl.warehouse import incremental_load_warehouse
from etl.star_schema import incremental_load_star_schema
from util.batch_id import get_batch_id, set_batch_id

@prefect.task(name="initial-warehouse-load")
def initial_load(
    batch_id: int,
):
    incremental_load_warehouse(batch_id)
    incremental_load_star_schema()

@prefect.task(name="incremental-warehouse-load")
def incremental_load(
    batch_id: int,
):
    incremental_load_warehouse(batch_id)
    incremental_load_star_schema()


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
    