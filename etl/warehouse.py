#!/usr/bin/env python3

"""
ETL for the warehouse database.
"""

import constants
import data.csv
import database
import database.reldb as reldb
import database.warehouse as warehouse
import database.csv_staging as csv_staging
import etl.utils as utils
import model.warehouse as warehouse_model
import model.reldb as reldb_model
import data

import sqlalchemy
import sqlalchemy.orm
from datetime import datetime

# reldb_tables = {
#     "pilots": reldb_model.Pilot,
#     "cabin_crew": reldb_model.CabinCrew,
#     "customers": reldb_model.Customer,
#     "airports": reldb_model.Airport,
#     "airplanes": reldb_model.Airplane,
#     "flights": reldb_model.Flight,
#     "flight_cabin_crew": reldb_model.FlightCabinCrew,
# }

departure_airport = sqlalchemy.orm.aliased(warehouse_model.Airport, name="departure_airport")
arrival_airport = sqlalchemy.orm.aliased(warehouse_model.Airport, name="arrival_airport")
pilot = sqlalchemy.orm.aliased(warehouse_model.Pilot, name="pilot")
copilot = sqlalchemy.orm.aliased(warehouse_model.Pilot, name="copilot")

select_map: dict[str, sqlalchemy.Select] = {
    'pilots': sqlalchemy.select(
        reldb_model.Pilot.id.label("pilot_id"),
        reldb_model.Pilot.name,
        reldb_model.Pilot.license_number,
    ),
    'cabin_crew': sqlalchemy.select(
        reldb_model.CabinCrew.id.label("cabin_crew_id"),
        reldb_model.CabinCrew.name,
        reldb_model.CabinCrew.employee_id,
    ),
    'customers': sqlalchemy.select(
        reldb_model.Customer.id.label("customer_id"),
        reldb_model.Customer.full_name,
        reldb_model.Customer.email,
        reldb_model.Customer.frequent_flyer,
    ),
    'airports': sqlalchemy.select(
        reldb_model.Airport.id.label("airport_id"),
        reldb_model.Airport.code,
        reldb_model.Airport.name,
        reldb_model.Airport.city,
        reldb_model.Airport.country,
    ),
    'airplanes': sqlalchemy.select(
        reldb_model.Airplane.id.label("airplane_id"),
        reldb_model.Airplane.model,
        reldb_model.Airplane.registration_number,
        reldb_model.Airplane.fuel_consumption_per_hour,
        reldb_model.Airplane.maintenance_days,
    ),
    'flights': sqlalchemy.select(
            reldb_model.Flight.id.label("flight_id"),
            reldb_model.Flight.flight_number,
            departure_airport.airport_sk.label("departure_airport_sk"),
            departure_airport.airport_id.label("departure_airport_id"),
            arrival_airport.airport_sk.label("arrival_airport_sk"),
            arrival_airport.airport_id.label("arrival_airport_id"),
            reldb_model.Flight.departure_time,
            reldb_model.Flight.arrival_time,
            reldb_model.Flight.delay_minutes,
            reldb_model.Flight.status,
            pilot.pilot_sk.label("pilot_sk"),
            pilot.pilot_id.label("pilot_id"),
            copilot.pilot_sk.label("copilot_sk"),
            copilot.pilot_id.label("copilot_id"),
            warehouse_model.Airplane.airplane_sk.label("airplane_sk"),
            reldb_model.Flight.is_ferry_flight,
            reldb_model.Flight.estimated_flight_hours,
        ).join(
            departure_airport,
            reldb_model.Flight.departure_airport_id == departure_airport.airport_id,
        ).join(
            arrival_airport,
            reldb_model.Flight.arrival_airport_id == arrival_airport.airport_id,
        ).join(
            pilot,
            reldb_model.Flight.pilot_id == pilot.pilot_id,
        ).join(
            copilot,
            reldb_model.Flight.copilot_id == copilot.pilot_id,
        ).join(
            warehouse_model.Airplane,
            reldb_model.Flight.airplane_id == warehouse_model.Airplane.airplane_id,
        ),
    'flight_cabin_crew': sqlalchemy.select(
        warehouse_model.CabinCrew.cabin_crew_sk,
        warehouse_model.CabinCrew.cabin_crew_id,
        warehouse_model.Flight.flight_sk,
        warehouse_model.Flight.flight_id,
        reldb_model.FlightCabinCrew.id.label("flight_cabin_crew_id"),
    ).join(
        warehouse_model.CabinCrew,
        reldb_model.FlightCabinCrew.cabin_crew_id == warehouse_model.CabinCrew.cabin_crew_id,
    ).join(
        warehouse_model.Flight,
        reldb_model.FlightCabinCrew.flight_id == warehouse_model.Flight.flight_id,
    ),
    'flight_bookings': sqlalchemy.select(
        reldb_model.FlightBooking.id.label("flight_booking_id"),
        warehouse_model.Flight.flight_sk,
        warehouse_model.Customer.customer_sk,
        reldb_model.FlightBooking.seat_number,
    ).join(
        warehouse_model.Flight,
        reldb_model.FlightCabinCrew.flight_id == warehouse_model.Flight.flight_id,
    ),
    'flight_bookings': sqlalchemy.select(
        reldb_model.FlightBooking.id.label("flight_booking_id"),
        warehouse_model.Flight.flight_sk,
        warehouse_model.Customer.customer_sk,
        reldb_model.FlightBooking.seat_number,
    ).join(
        warehouse_model.Flight,
        reldb_model.FlightBooking.flight_id == warehouse_model.Flight.flight_id,
    ).join(
        warehouse_model.Customer,
        reldb_model.FlightBooking.customer_id == warehouse_model.Customer.customer_id,
    ),
}

id_map = {
    'pilots': [reldb_model.Pilot.id, warehouse_model.Pilot.pilot_id],
    'cabin_crew': [reldb_model.CabinCrew.id, warehouse_model.CabinCrew.cabin_crew_id],
    'customers': [reldb_model.Customer.id, warehouse_model.Customer.customer_id],
    'airports': [reldb_model.Airport.id, warehouse_model.Airport.airport_id],
    'airplanes': [reldb_model.Airplane.id, warehouse_model.Airplane.airplane_id],
    'flights': [reldb_model.Flight.id, warehouse_model.Flight.flight_id],
    'flight_cabin_crew': [reldb_model.FlightCabinCrew.id, warehouse_model.FlightCabinCrew.flight_cabin_crew_id],
    'flight_bookings': [reldb_model.FlightBooking.id, warehouse_model.FlightBooking.flight_booking_id],
}

def reset_warehouse_schema(
        engine: sqlalchemy.engine.Engine,
        metadata: sqlalchemy.MetaData,
):
    print(" --- Wiping warehouse schema ---")
    database.wipe_schema(engine, metadata)
    database.ensure_schema(engine, metadata)

    print(" --- Recreating warehouse schema ---")
    database.ensure_schema(engine, metadata)

    print(" +++ Warehouse schema reset successfully +++ ")

def full_load_warehouse_2(
        insert_id: int,
):
    print(" --- Starting full load of warehouse ---")
    warehouse_session = database.get_session(warehouse.engine)

    reset_warehouse_schema(warehouse.engine, warehouse.metadata)

    for table_name, select_stmt in select_map.items():
        print(f" --- Loading {table_name} table ---")
        warehouse_table = warehouse.metadata.tables[f"{warehouse.metadata.schema}.{table_name}"]

        assert isinstance(warehouse_table, sqlalchemy.Table), f"Table {table_name} is not a valid table"

        insert_stmt = utils.create_warehouse_insert_stmt(
            insert_id,
            constants.WAREHOUSE_RELDB_SOURCE_ID,
            select_stmt,
            warehouse_table,
        )

        print(insert_stmt)
        warehouse_session.execute(insert_stmt)
        print(f" +++ {table_name} table loaded successfully +++ ")

    warehouse_session.commit()

    incremental_load_csv_staging(insert_id, "data/output/reviews.csv")
    print(" +++ Full load of warehouse completed successfully +++ ")


def full_load_warehouse(
        insert_id: int,
):
    print(" --- Starting full load of warehouse ---")
    reldb_session = database.get_session(reldb.engine)
    warehouse_session = database.get_session(warehouse.engine)

    # --- Step 1: Wipe warehouse clean ---
    database.wipe_schema(warehouse.engine, warehouse.metadata)

    # --- Step 2: Recreate warehouse schema ---
    database.ensure_schema(warehouse.engine, warehouse.metadata)


    # define constants
    insert_id_l = sqlalchemy.literal(insert_id).label("insert_id")
    update_id_l = sqlalchemy.sql.null().label("update_id")
    source_id_l = sqlalchemy.literal(constants.WAREHOUSE_RELDB_SOURCE_ID).label("source_id")
    start_date_l = sqlalchemy.literal(datetime.now()).label("start_date")
    end_date_l = sqlalchemy.literal(datetime.max).label("end_date")

    constants_names = [
        insert_id_l.name,
        update_id_l.name,
        source_id_l.name,
        start_date_l.name,
        end_date_l.name,
    ]

    constants_columns = [
        insert_id_l,
        update_id_l,
        source_id_l,
        start_date_l,
        end_date_l,
    ]

    # ----- PILOTS TABLE -----

    print(" --- Loading pilots table ---")

    select_stmt = sqlalchemy.select(
        reldb_model.Pilot.id.label("pilot_id"),
        reldb_model.Pilot.name,
        reldb_model.Pilot.license_number,
        *constants_columns,
    )
    insert_stmt = sqlalchemy.insert(warehouse_model.Pilot).from_select(
        [
            "pilot_id",
            "name",
            "license_number",
            *constants_names,
        ],
        select_stmt,
    )
    warehouse_session.execute(insert_stmt)

    # ----- CABIN CREW TABLE -----

    print(" +++ Pilots table loaded successfully +++ ")

    print(" --- Loading cabin crew table ---")

    select_stmt = sqlalchemy.select(
        reldb_model.CabinCrew.id.label("cabin_crew_id"),
        reldb_model.CabinCrew.name,
        reldb_model.CabinCrew.employee_id,
        *constants_columns,
    )
    insert_stmt = sqlalchemy.insert(warehouse_model.CabinCrew).from_select(
        [
            "cabin_crew_id",
            "name",
            "employee_id",
            *constants_names,
        ],
        select_stmt,
    )
    warehouse_session.execute(insert_stmt)

    # ----- CUSTOMERS TABLE -----

    print(" +++ Cabin crew table loaded successfully +++ ")
    print(" --- Loading customers table ---")

    select_stmt = sqlalchemy.select(
        reldb_model.Customer.id.label("customer_id"),
        reldb_model.Customer.full_name,
        reldb_model.Customer.email,
        reldb_model.Customer.frequent_flyer,
        *constants_columns,
    )
    insert_stmt = sqlalchemy.insert(warehouse_model.Customer).from_select(
        [
            "customer_id",
            "full_name",
            "email",
            "frequent_flyer",
            *constants_names,
        ],
        select_stmt,
    )
    warehouse_session.execute(insert_stmt)

    # ----- AIRPORTS TABLE -----

    print(" +++ Customers table loaded successfully +++ ")
    print(" --- Loading airports table ---")

    select_stmt = sqlalchemy.select(
        reldb_model.Airport.id.label("airport_id"),
        reldb_model.Airport.code,
        reldb_model.Airport.name,
        reldb_model.Airport.city,
        reldb_model.Airport.country,
        *constants_columns,
    )
    insert_stmt = sqlalchemy.insert(warehouse_model.Airport).from_select(
        [
            "airport_id",
            "code",
            "name",
            "city",
            "country",
            *constants_names,
        ],
        select_stmt,
    )

    warehouse_session.execute(insert_stmt)

    # ----- AIRPLANES TABLE -----

    print(" +++ Airports table loaded successfully +++ ")
    print(" --- Loading airplanes table ---")

    select_stmt = sqlalchemy.select(
        reldb_model.Airplane.id.label("airplane_id"),
        reldb_model.Airplane.model,
        reldb_model.Airplane.registration_number,
        reldb_model.Airplane.fuel_consumption_per_hour,
        reldb_model.Airplane.maintenance_days,
        *constants_columns,
    )
    insert_stmt = sqlalchemy.insert(warehouse_model.Airplane).from_select(
        [
            "airplane_id",
            "model",
            "registration_number",
            "fuel_consumption_per_hour",
            "maintenance_days",
            *constants_names,
        ],
        select_stmt,
    )
    warehouse_session.execute(insert_stmt)

    # ----- FLIGHTS TABLE -----

    print(" +++ Airplanes table loaded successfully +++ ")
    print(" --- Loading flights table ---")

    select_stmt = sqlalchemy.select(
        reldb_model.Flight.id.label("flight_id"),
        reldb_model.Flight.flight_number,
        warehouse_model.Airport.airport_sk.label("departure_airport_sk"),
        warehouse_model.Airport.airport_sk.label("arrival_airport_sk"),
        reldb_model.Flight.departure_time,
        reldb_model.Flight.arrival_time,
        reldb_model.Flight.delay_minutes,
        reldb_model.Flight.status,
        warehouse_model.Pilot.pilot_sk.label("pilot_sk"),
        warehouse_model.Pilot.pilot_sk.label("copilot_sk"),
        warehouse_model.Airplane.airplane_sk.label("airplane_sk"),
        reldb_model.Flight.is_ferry_flight,
        reldb_model.Flight.estimated_flight_hours,
        *constants_columns,
    ).join(
        warehouse_model.Airport,
        sqlalchemy.or_(
            reldb_model.Flight.departure_airport_id == warehouse_model.Airport.airport_id,
            reldb_model.Flight.arrival_airport_id == warehouse_model.Airport.airport_id,
        )
    ).join(
        warehouse_model.Pilot,
        sqlalchemy.or_(
            reldb_model.Flight.pilot_id == warehouse_model.Pilot.pilot_id,
            reldb_model.Flight.copilot_id == warehouse_model.Pilot.pilot_id,
        )
    ).join(
        warehouse_model.Airplane,
        reldb_model.Flight.airplane_id == warehouse_model.Airplane.airplane_id,
    )
    insert_stmt = sqlalchemy.insert(warehouse_model.Flight).from_select(
        [
            "flight_id",
            "flight_number",
            "departure_airport_sk",
            "arrival_airport_sk",
            "departure_time",
            "arrival_time",
            "delay_minutes",
            "status",
            "pilot_sk",
            "copilot_sk",
            "airplane_sk",
            "is_ferry_flight",
            "estimated_flight_hours",
            *constants_names,
        ],
        select_stmt,
    )
    warehouse_session.execute(insert_stmt)

    # ----- FLIGHT CABIN CREW TABLE -----

    print(" +++ Flights table loaded successfully +++ ")
    print(" --- Loading flight cabin crew table ---")

    select_stmt = sqlalchemy.select(
        reldb_model.FlightCabinCrew.id.label("flight_cabin_crew_id"),
        warehouse_model.Flight.flight_sk,
        warehouse_model.CabinCrew.cabin_crew_sk.label("cabin_crew_sk"),
        *constants_columns,
    ).join(
        warehouse_model.CabinCrew,
        reldb_model.FlightCabinCrew.cabin_crew_id == warehouse_model.CabinCrew.cabin_crew_id,
    ).join(
        warehouse_model.Flight,
        reldb_model.FlightCabinCrew.flight_id == warehouse_model.Flight.flight_id,
    )
    insert_stmt = sqlalchemy.insert(warehouse_model.FlightCabinCrew).from_select(
        [
            "flight_cabin_crew_id",
            "flight_sk",
            "cabin_crew_sk",
            *constants_names,
        ],
        select_stmt,
    )
    warehouse_session.execute(insert_stmt)

    # ----- FLIGHT BOOKINGS TABLE -----

    print(" +++ Flight cabin crew table loaded successfully +++ ")
    print(" --- Loading flight bookings table ---")

    select_stmt = sqlalchemy.select(
        reldb_model.FlightBooking.id.label("flight_booking_id"),
        warehouse_model.Flight.flight_sk,
        warehouse_model.Customer.customer_sk,
        reldb_model.FlightBooking.seat_number,
        *constants_columns,
    ).join(
        warehouse_model.Flight,
        reldb_model.FlightBooking.flight_id == warehouse_model.Flight.flight_id,
    ).join(
        warehouse_model.Customer,
        reldb_model.FlightBooking.customer_id == warehouse_model.Customer.customer_id,
    )
    insert_stmt = sqlalchemy.insert(warehouse_model.FlightBooking).from_select(
        [
            "flight_booking_id",
            "flight_sk",
            "customer_sk",
            "seat_number",
            *constants_names,
        ],
        select_stmt,
    )
    warehouse_session.execute(insert_stmt)

    print(" +++ Flight bookings table loaded successfully +++ ")

    incremental_load_csv_staging(insert_id, "data/output/reviews.csv")
    print(" +++ Full load of warehouse completed successfully +++ ")

    warehouse_session.commit()


def generate_incremental_load_stmts(
        batch_id: int,
        source_id: int,
        wh_table: sqlalchemy.Table,
        op_table: sqlalchemy.Table,
        wh_id_col: sqlalchemy.orm.InstrumentedAttribute,
        comparison_tuples: list[tuple[sqlalchemy.orm.InstrumentedAttribute, sqlalchemy.orm.InstrumentedAttribute]],
        select_stmt: sqlalchemy.Select,
): 
    print(f" --- Generating incremental load stmts for table: {wh_table.name} ---")

    diff_condition = utils.generate_diff_condition(
            wh_table,
            op_table,
            { 
                "pilot_sk", 
                "copilot_sk", 
                # "pilot_id", 
                "cabin_crew_sk", 
                # "cabin_crew_id", 
                "customer_sk", 
                # "customer_id", 
                "airport_sk", 
                # "airport_id", 
                "airplane_sk", 
                # "airplane_id",
                "flight_sk",
                # "flight_id",
                "flight_cabin_crew_sk",
                # "flight_cabin_crew_id",
                "flight_booking_sk",
                # "flight_booking_id",
                "airline_review_sk",
                "departure_airport_sk",
                "arrival_airport_sk",
                "review_TEMP_PK",
                "review_sk",
            }
        )

    if diff_condition is  None:
        raise ValueError("No diff condition found for table: " + wh_table.name)

    update_stmt = sqlalchemy.update(wh_table).where(
        sqlalchemy.and_(
                diff_condition,
                wh_table.c.end_date == sqlalchemy.literal(datetime.max),
                sqlalchemy.and_(*[op_col == wh_col for op_col, wh_col in comparison_tuples]),
            )
    ).values(
        end_date=sqlalchemy.literal(datetime.now()),
        update_id=sqlalchemy.literal(batch_id),
        source_id=sqlalchemy.literal(source_id),
    )

    #insert new values
    insert_stmt = utils.create_warehouse_insert_stmt(
        batch_id,
        source_id,
        select_stmt.join(
            wh_table,
            sqlalchemy.and_(
               wh_table.c.end_date == sqlalchemy.literal(datetime.max),
               sqlalchemy.and_(*[op_col == wh_col for op_col, wh_col in comparison_tuples]),
            ),
            isouter=True,
        ).where(
            wh_id_col == sqlalchemy.null(),
        ),
        wh_table,
    )

    print(f" +++ Incremental load stmts generated successfully for table: {wh_table.name} +++ ")

    return insert_stmt, update_stmt


def incremental_load_warehouse(
        batch_id: int,
):
    print(" --- Starting incremental load of warehouse ---")

    warehouse_session = database.get_session(warehouse.engine)

    for table_name, select_stmt in select_map.items():
        print(f" --- Loading table: {table_name} ---")
        wh_table = warehouse.metadata.tables[f"{warehouse.metadata.schema}.{table_name}"]
        assert isinstance(wh_table, sqlalchemy.Table), "wh_table must be a sqlalchemy.Table"

        op_table = reldb.metadata.tables[f"{reldb.metadata.schema}.{table_name}"]
        assert isinstance(op_table, sqlalchemy.Table), "op_table must be a sqlalchemy.Table"

        reldb_id_col, wh_id_col = id_map[table_name]

        insert_stmt, update_stmt = generate_incremental_load_stmts(
            batch_id,
            constants.WAREHOUSE_RELDB_SOURCE_ID,
            wh_table,
            op_table,
            wh_id_col,
            [(reldb_id_col, wh_id_col)],
            select_stmt,
        )
        print(f" --- Executing update stmt for table: {table_name} ---")
        print(update_stmt)
        warehouse_session.execute(update_stmt)
        print(f" --- Executing insert stmt for table: {table_name} ---")
        print(insert_stmt)
        warehouse_session.execute(insert_stmt)
        print(f" +++ Incremental load of table: {table_name} completed successfully +++ ")


    incremental_load_csv_staging(batch_id, "data/output/reviews.csv")
    print(" +++ Incremental load of warehouse completed successfully +++ ")

    warehouse_session.commit()


def incremental_load_csv_staging(
        batch_id: int,
        fname: str,
):
    print(" --- Starting incremental load of csv staging ---")

    warehouse_session = database.get_session(warehouse.engine)

    print(" --- Wiping csv staging schema ---")
    # clear csv staging schema
    database.wipe_schema(csv_staging.engine, csv_staging.metadata)

    print(" --- Recreating csv staging schema ---")
    # recreate csv staging schema
    database.ensure_schema(csv_staging.engine, csv_staging.metadata)

    print(" --- Inserting csv into csv staging table ---")
    #hardcode, because theres only 1 csv table

    #first, insert csv into the staging table
    reviews = data.csv.parse_our_reviews(fname)

    # warehouse_session.execute(sqlalchemy.insert(csv_staging.AirlineReview).values(
    #     [csv_staging.AirlineReview(
    #             flight_id=review.flight_id,
    #             customer_id=review.customer_id,
    #             seat_class=review.seat_class,
    #             content=review.content,
    #             rating=review.rating,
    #             recommended=review.recommended,
    #             seat_comfort=review.seat_comfort,
    #             cabin_staff_service=review.cabin_staff_service,
    #             food_and_beverages=review.food_and_beverages,
    #             inflight_entertainment=review.inflight_entertainment,
    #             value_for_money=review.value_for_money,
    #             date_published=review.date_published,
    #         )
    #         for review in reviews
    #     ]
    # ))
    try:
        warehouse_session.bulk_save_objects(
            [
        csv_staging.AirlineReview(
                flight_id=review.flight_id,
                customer_id=review.customer_id,
                seat_class=review.seat_class,
                content=review.content,
                rating=review.rating,
                recommended=review.recommended,
                seat_comfort=review.seat_comfort,
                cabin_staff_service=review.cabin_staff_service,
                food_and_beverages=review.food_and_beverages,
                inflight_entertainment=review.inflight_entertainment,
                value_for_money=review.value_for_money,
                date_published=review.date_published,
            )
            for review in reviews
        ]
        )
    except Exception as e:
        print(e)
        raise e

    warehouse_session.commit()

    print(" --- Generating update and insert statements for load ---")
    #now, generate update and insert statements for load

    print(" --- Updating warehouse table ---")
    wh_table = warehouse.metadata.tables[f"{warehouse.metadata.schema}.{warehouse_model.AirlineReview.__tablename__}"]
    op_table = csv_staging.metadata.tables[f"{csv_staging.metadata.schema}.{csv_staging.AirlineReview.__tablename__}"]

    wh_table_t = warehouse_model.AirlineReview
    op_table_t = csv_staging.AirlineReview
    # airline reviews composite key:
    # (flight_id, customer_id, date_published,)

    print(" --- Executing update and insert statements ---")
    insert_stmt, update_stmt = generate_incremental_load_stmts(
        batch_id,
        constants.WAREHOUSE_CSV_SOURCE_ID,
        wh_table,
        op_table,
        wh_table_t.airline_review_sk,
        [(op_table_t.flight_id, wh_table_t.flight_id), (op_table_t.customer_id, wh_table_t.customer_id), (op_table_t.date_published, wh_table_t.date_published)],
        sqlalchemy.select(
            op_table_t.flight_id,
            warehouse_model.Flight.flight_sk,
            op_table_t.customer_id,
            warehouse_model.Customer.customer_sk,
            op_table_t.seat_class,
            op_table_t.content,
            op_table_t.rating,
            op_table_t.recommended,
            op_table_t.seat_comfort,
            op_table_t.cabin_staff_service,
            op_table_t.food_and_beverages,
            op_table_t.inflight_entertainment,
            op_table_t.value_for_money,
            op_table_t.date_published,
        ).join(
            warehouse_model.Flight,
            op_table_t.flight_id == warehouse_model.Flight.flight_id,
        ).join(
            warehouse_model.Customer,
            op_table_t.customer_id == warehouse_model.Customer.customer_id,
        )
    )

    print(update_stmt)
    print(insert_stmt)
    warehouse_session.execute(update_stmt)
    warehouse_session.execute(insert_stmt)

    print(" --- Committing changes ---")
    warehouse_session.commit()

    print(" +++ Incremental load of csv staging completed successfully +++ ")

    #wipe staging schema
    print(" --- Wiping csv staging schema ---")
    database.wipe_schema(csv_staging.engine, csv_staging.metadata)


if __name__ == "__main__":
    # full_load_warehouse(1)
    #test incremental load

    # session = database.get_session(reldb.engine)
    # # change = sqlalchemy.update(reldb_model.Pilot).where(
    # #     reldb_model.Pilot.id == 196,
    # # ).values(
    # #     name="John Doe",
    # # )
    # wh_table = warehouse.metadata.tables[f"{warehouse.metadata.schema}.pilots"]
    # change = select_map['pilots'].join(
    #             wh_table,
    #             sqlalchemy.and_(
    #                 reldb_model.Pilot.id == wh_table.c.pilot_id,
    #                 wh_table.c.end_date == sqlalchemy.literal(datetime.max),
    #             ),
    #             isouter=True,
    #         ).where(
    #             wh_table.c.pilot_id == sqlalchemy.null(),
    #         )
    # print(change)
    # res = session.execute(change)
    # print(res.all())
    # session.commit()

    reset_warehouse_schema(warehouse.engine, warehouse.metadata)
    incremental_load_warehouse(2)
    # incremental_load_csv_staging(2, "data/output/reviews.csv")