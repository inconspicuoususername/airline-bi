import copy
import database
import database.reldb as reldb
import database.warehouse as warehouse
import model.warehouse as warehouse_model
import model.reldb as reldb_model
import sqlalchemy
from datetime import datetime

csv_source_id = 1
reldb_source_id = 2

# reldb_tables = {
#     "pilots": reldb_model.Pilot,
#     "cabin_crew": reldb_model.CabinCrew,
#     "customers": reldb_model.Customer,
#     "airports": reldb_model.Airport,
#     "airplanes": reldb_model.Airplane,
#     "flights": reldb_model.Flight,
#     "flight_cabin_crew": reldb_model.FlightCabinCrew,
# }

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
    ),
    'flight_cabin_crew': sqlalchemy.select(
        reldb_model.FlightCabinCrew.id.label("flight_cabin_crew_id"),
        warehouse_model.Flight.flight_sk,
        warehouse_model.CabinCrew.cabin_crew_sk.label("cabin_crew_sk"),
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

def full_load_warehouse_2(
        insert_id: int,
):
    print(" --- Starting full load of warehouse ---")
    warehouse_session = database.get_session(warehouse.engine)

    # --- Step 1: Wipe warehouse clean ---
    database.wipe_schema(warehouse.engine, warehouse.metadata)

    # --- Step 2: Recreate warehouse schema ---
    database.ensure_schema(warehouse.engine, warehouse.metadata)


    # define constants
    insert_id_l = sqlalchemy.literal(insert_id).label("insert_id")
    update_id_l = sqlalchemy.sql.null().label("update_id")
    source_id_l = sqlalchemy.literal(reldb_source_id).label("source_id")
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

    # ----- FLIGHT BOOKINGS TABLE -----
    for table_name, select_stmt in select_map.items():
        print(f" --- Loading {table_name} table ---")
        warehouse_table = warehouse.metadata.tables[table_name]

        assert isinstance(warehouse_table, sqlalchemy.Table), f"Table {table_name} is not a valid table"

        select_stmt = select_stmt.add_columns(*constants_columns)
        insert_stmt = sqlalchemy.insert(warehouse_table).from_select(
            [
                *warehouse_table.columns.keys(),
                *constants_names,
            ],
            select_stmt,
        )
        warehouse_session.execute(insert_stmt)
        print(f" +++ {table_name} table loaded successfully +++ ")

    
    print(" +++ Full load of warehouse completed successfully +++ ")

    warehouse_session.commit()

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
    source_id_l = sqlalchemy.literal(reldb_source_id).label("source_id")
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
    print(" +++ Full load of warehouse completed successfully +++ ")

    warehouse_session.commit()




def incremental_load_warehouse():
    pass

if __name__ == "__main__":
    full_load_warehouse(1)
    incremental_load_warehouse()