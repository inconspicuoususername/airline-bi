#!/usr/bin/env python3

"""
ETL for the star schema.
"""

import datetime
import sqlalchemy
import sqlalchemy.orm
import database
import database.star_schema as star_db
import model.star_schema as star
import model.warehouse as whm
import etl.utils as utils
from sqlalchemy.dialects.postgresql import insert as pg_insert  # For ON CONFLICT

departure_date_table = sqlalchemy.orm.aliased(star.DimDate, name="departure_date")
arrival_date_table = sqlalchemy.orm.aliased(star.DimDate, name="arrival_date")

select_map: dict[str, sqlalchemy.Select] = {
    'dim_airport': sqlalchemy.select(
        whm.Airport.airport_sk,
        whm.Airport.code,
        whm.Airport.name,
        whm.Airport.city,
        whm.Airport.country,
    ),
    'dim_airplane': sqlalchemy.select(
        whm.Airplane.airplane_sk,
        whm.Airplane.model,
        whm.Airplane.registration_number,
        whm.Airplane.fuel_consumption_per_hour,
        whm.Airplane.maintenance_days,
    ),
    'dim_pilot': sqlalchemy.select(
        whm.Pilot.pilot_sk,
        whm.Pilot.name,
        whm.Pilot.license_number,
    ),
    'dim_customer': sqlalchemy.select(
        whm.Customer.customer_sk,
        whm.Customer.full_name,
        whm.Customer.email,
        whm.Customer.frequent_flyer,
    ),
    'dim_flight':sqlalchemy.select(
        whm.Flight.flight_sk,
        whm.Flight.flight_number,
        whm.Flight.departure_airport_sk,
        whm.Flight.arrival_airport_sk,
        whm.Flight.departure_time.label("departure_datetime"),
        whm.Flight.arrival_time.label("arrival_datetime"),
        whm.Flight.airplane_sk,
        whm.Flight.pilot_sk,
        whm.Flight.copilot_sk,
        whm.Flight.status,
        whm.Flight.is_ferry_flight,
    ),
    'fact_flight': sqlalchemy.select(
            whm.Flight.flight_sk,
            departure_date_table.date_sk.label("departure_date_sk"),
            arrival_date_table.date_sk.label("arrival_date_sk"),
            whm.Flight.delay_minutes,
            whm.Flight.estimated_flight_hours,
            whm.Flight.is_ferry_flight,
        ).join(
            departure_date_table,
            sqlalchemy.cast(whm.Flight.departure_time, sqlalchemy.Date) == departure_date_table.date,
        ).join(
            arrival_date_table,
            sqlalchemy.cast(whm.Flight.arrival_time, sqlalchemy.Date) == arrival_date_table.date,
        ),
    # 'fact_booking': sqlalchemy.select(
    #         whm.FlightBooking.flight_booking_sk,
    #         star.DimFlight.flight_sk,
    #         star.DimCustomer.customer_sk,
    #         whm.FlightBooking.seat_number,
    #     ).join(
    #         star.DimFlight,
    #         whm.FlightBooking.flight_sk == star.DimFlight.flight_sk,
    #     ).join(
    #         star.DimCustomer,
    #         whm.FlightBooking.customer_sk == star.DimCustomer.customer_sk,
    #     ),
    'fact_booking': sqlalchemy.select(
        whm.FlightBooking.flight_booking_sk,
        whm.FlightBooking.flight_sk,
        whm.FlightBooking.customer_sk,
        whm.FlightBooking.seat_number,
    ),
    'fact_review': sqlalchemy.select(
        whm.AirlineReview.airline_review_sk,
        whm.AirlineReview.flight_sk,
        whm.AirlineReview.customer_sk,
        whm.AirlineReview.seat_class,
        whm.AirlineReview.rating,
        whm.AirlineReview.recommended,
        whm.AirlineReview.seat_comfort,
        whm.AirlineReview.cabin_staff_service,
        whm.AirlineReview.food_and_beverages,
        whm.AirlineReview.inflight_entertainment,
        whm.AirlineReview.value_for_money,
        star.DimDate.date_sk.label("date_published_sk"),
    ).join(
        star.DimFlight,
        whm.AirlineReview.flight_sk == star.DimFlight.flight_sk,
    ).join(
        star.DimCustomer,
        whm.AirlineReview.customer_sk == star.DimCustomer.customer_sk,
    ).join(
        star.DimDate,
        sqlalchemy.cast(whm.AirlineReview.date_published, sqlalchemy.Date) == star.DimDate.date,
    )
}

def incremental_load_star_schema():
    print("--- Begin incremental load on star schema ---")
    session = database.get_session(star_db.engine)
    # delete_stmt = sqlalchemy.delete(star.DimDate)
    # session.execute(delete_stmt)
    # session.commit()

    #delete facts and dimensions that have fks and are out of
    print("--- Deleting tables with fks ---")
    print("--- Fact review ---")
    delete_stmt = sqlalchemy.delete(star.FactReview).where(
        sqlalchemy.and_(
            whm.AirlineReview.airline_review_sk == star.FactReview.airline_review_sk,
            whm.AirlineReview.end_date != sqlalchemy.literal(datetime.datetime.max),
        )
    )

    print(delete_stmt)
    session.execute(delete_stmt)

    print("--- Fact booking ---")
    delete_stmt = sqlalchemy.delete(star.FactBooking).where(
        sqlalchemy.and_(
            whm.FlightBooking.flight_booking_sk == star.FactBooking.flight_booking_sk,
            whm.FlightBooking.end_date != sqlalchemy.literal(datetime.datetime.max),
        )
    )
    print(delete_stmt)
    session.execute(delete_stmt)

    print("--- Fact flight ---")
    delete_stmt = sqlalchemy.delete(star.FactFlight).where(
        sqlalchemy.and_(
            whm.Flight.flight_sk == star.FactFlight.flight_sk,
            whm.Flight.end_date != sqlalchemy.literal(datetime.datetime.max),
        )
    )
    print(delete_stmt)
    session.execute(delete_stmt)

    print("--- Dim flight ---")
    delete_stmt = sqlalchemy.delete(star.DimFlight).where(
        sqlalchemy.and_(
            whm.Flight.flight_sk == star.DimFlight.flight_sk,
            whm.Flight.end_date != sqlalchemy.literal(datetime.datetime.max),
        )
    )
    print(delete_stmt)
    session.execute(delete_stmt)

    #delete and reconstruct other dimensions

    print("--- Deleting and reconstructing other dimensions ---")

    #-- dim airport 
    #-- delete
    delete_stmt = sqlalchemy.delete(star.DimAirport).where(
        sqlalchemy.and_(
            whm.Airport.airport_sk == star.DimAirport.airport_sk,
            whm.Airport.end_date != sqlalchemy.literal(datetime.datetime.max),
        )
    )
    print(delete_stmt)
    session.execute(delete_stmt)

    #-- insert
    select_stmt = select_map['dim_airport']
    insert_stmt = pg_insert(star.DimAirport).from_select(
        [
            star.DimAirport.airport_sk,
            star.DimAirport.code,
            star.DimAirport.name,
            star.DimAirport.city,
            star.DimAirport.country,
        ],
        select_stmt.where(
            whm.Airport.end_date == sqlalchemy.literal(datetime.datetime.max),
        )
    ).on_conflict_do_nothing(
        index_elements=[star.DimAirport.airport_sk]
    )


    session.execute(insert_stmt)

    #-- dim airplane
        
    delete_stmt = sqlalchemy.delete(star.DimAirplane).where(
        sqlalchemy.and_(
            whm.Airplane.airplane_sk == star.DimAirplane.airplane_sk,
            whm.Airplane.end_date != sqlalchemy.literal(datetime.datetime.max),
        )
    )
    print(delete_stmt)
    session.execute(delete_stmt)

    #-- insert
    select_stmt = select_map['dim_airplane']
    insert_stmt = pg_insert(star.DimAirplane).from_select(
        [
            star.DimAirplane.airplane_sk,
            star.DimAirplane.model,
            star.DimAirplane.registration_number,
            star.DimAirplane.fuel_consumption_per_hour,
            star.DimAirplane.maintenance_days,
        ],
        select_stmt.where(
            whm.Airplane.end_date == sqlalchemy.literal(datetime.datetime.max),
        )
    ).on_conflict_do_nothing(
        index_elements=[star.DimAirplane.airplane_sk]
    )

    session.execute(insert_stmt)

    #-- dim pilot
    delete_stmt = sqlalchemy.delete(star.DimPilot).where(
        sqlalchemy.and_(
            whm.Pilot.pilot_sk == star.DimPilot.pilot_sk,
            whm.Pilot.end_date != sqlalchemy.literal(datetime.datetime.max),
        )
    )
    print(delete_stmt)
    session.execute(delete_stmt)

    #-- insert
    select_stmt = select_map['dim_pilot']
    insert_stmt = pg_insert(star.DimPilot).from_select(
        [
            star.DimPilot.pilot_sk,
            star.DimPilot.name,
            star.DimPilot.license_number,
        ],
        select_stmt.where(
            whm.Pilot.end_date == sqlalchemy.literal(datetime.datetime.max),
        )
    ).on_conflict_do_nothing(
        index_elements=[star.DimPilot.pilot_sk]
    )
    session.execute(insert_stmt)

    #-- dim customer
    delete_stmt = sqlalchemy.delete(star.DimCustomer).where(
        sqlalchemy.and_(
            whm.Customer.customer_sk == star.DimCustomer.customer_sk,
            whm.Customer.end_date != sqlalchemy.literal(datetime.datetime.max),
        )
    )
    print(delete_stmt)
    session.execute(delete_stmt)

    #-- insert
    select_stmt = select_map['dim_customer']
    insert_stmt = pg_insert(star.DimCustomer).from_select(
        [
            star.DimCustomer.customer_sk,
            star.DimCustomer.full_name,
            star.DimCustomer.email,
            star.DimCustomer.frequent_flyer,
        ],
        select_stmt.where(
            whm.Customer.end_date == sqlalchemy.literal(datetime.datetime.max),
        )
    ).on_conflict_do_nothing(
        index_elements=[star.DimCustomer.customer_sk]
    )
    print(insert_stmt)
    session.execute(insert_stmt)

    #-- dim flight - we have already deleted, so we can just insert
    print("--- Dim flight ---")
    #-- insert
    select_stmt = select_map['dim_flight']
    insert_stmt = pg_insert(star.DimFlight).from_select(
        [
            star.DimFlight.flight_sk,
            star.DimFlight.flight_number,
            star.DimFlight.departure_airport_sk,
            star.DimFlight.arrival_airport_sk,
            star.DimFlight.departure_datetime,
            star.DimFlight.arrival_datetime,
            star.DimFlight.airplane_sk,
            star.DimFlight.pilot_sk,
            star.DimFlight.copilot_sk,
            star.DimFlight.status,
            star.DimFlight.is_ferry_flight,
        ],
        select_stmt.where(
            whm.Flight.end_date == sqlalchemy.literal(datetime.datetime.max),
        )
    ).on_conflict_do_nothing(
        index_elements=[star.DimFlight.flight_sk]
    )
    print(insert_stmt)
    session.execute(insert_stmt)

    #-- we will ignore dim date for now seeing as it covers 100 years or so

    #-- fact flight
    select_stmt = select_map['fact_flight']
    insert_stmt = pg_insert(star.FactFlight).from_select(
        [
            star.FactFlight.flight_sk,
            star.FactFlight.departure_date_sk,
            star.FactFlight.arrival_date_sk,
            star.FactFlight.delay_minutes,
            star.FactFlight.estimated_flight_hours,
            star.FactFlight.is_ferry_flight,
        ],
        select_stmt.where(
            whm.Flight.end_date == sqlalchemy.literal(datetime.datetime.max),
        )
    ).on_conflict_do_nothing(
        index_elements=[star.FactFlight.flight_sk]
    )
    print(insert_stmt)
    session.execute(insert_stmt)

    #-- fact booking
    # write path overhead will kill the performance of half of this code but its fine,
    # this is the hottest table, using only anti joins here to avoid refactoring everything
    select_stmt = select_map['fact_booking']
    insert_stmt = pg_insert(star.FactBooking).from_select(
        [
            star.FactBooking.flight_booking_sk,
            star.FactBooking.flight_sk,
            star.FactBooking.customer_sk,
            star.FactBooking.seat_number,
        ],
        select_stmt.join(
            star.FactBooking,
            star.FactBooking.flight_booking_sk == whm.FlightBooking.flight_booking_sk,
        ).where(
            sqlalchemy.and_(
                whm.FlightBooking.end_date == sqlalchemy.literal(datetime.datetime.max),
                whm.FlightBooking.flight_booking_sk == sqlalchemy.null(),
            )
        )
    )
    print(insert_stmt)
    session.execute(insert_stmt)

    #-- fact review
    select_stmt = select_map['fact_review']
    insert_stmt = pg_insert(star.FactReview).from_select(
        [
            star.FactReview.airline_review_sk,
            star.FactReview.flight_sk,
            star.FactReview.customer_sk,
            star.FactReview.seat_class,
            star.FactReview.rating,
            star.FactReview.recommended,
            star.FactReview.seat_comfort,
            star.FactReview.cabin_staff_service,
            star.FactReview.food_and_beverages,
            star.FactReview.inflight_entertainment,
            star.FactReview.value_for_money,
            star.FactReview.date_published_sk,
        ],
        select_stmt.where(
            whm.AirlineReview.end_date == sqlalchemy.literal(datetime.datetime.max),
        )
    ).on_conflict_do_nothing(
        index_elements=[star.FactReview.airline_review_sk]
    )
    print(insert_stmt)
    session.execute(insert_stmt)


    session.commit()
    print("--- Star schema loaded ---")

def full_load_star_schema(
        batch_id: int,
):
    print("--- Begin full load on star schema ---")
    session = database.get_session(star_db.engine)

    print("--- Wiping and recreating schema (this may take a while on a large database) ---")
    database.wipe_schema(star_db.engine,star_db.metadata)
    database.ensure_schema(star_db.engine,star_db.metadata)


    print("--- Dim airport ---")
    select_stmt = sqlalchemy.select(
        whm.Airport.airport_sk,
        whm.Airport.code,
        whm.Airport.name,
        whm.Airport.city,
        whm.Airport.country,
    )
    insert_stmt = sqlalchemy.insert(star.DimAirport).from_select(
        [
            star.DimAirport.airport_sk,
            star.DimAirport.code,
            star.DimAirport.name,
            star.DimAirport.city,
            star.DimAirport.country,
        ],
        select_stmt,
    )
    session.execute(insert_stmt)
    print(insert_stmt)
    print("+++Dim airport loaded+++")

    print("--- Dim airplane ---")
    select_stmt = sqlalchemy.select(
        whm.Airplane.airplane_sk,
        whm.Airplane.model,
        whm.Airplane.registration_number,
        whm.Airplane.fuel_consumption_per_hour,
        whm.Airplane.maintenance_days,
    )
    insert_stmt = sqlalchemy.insert(star.DimAirplane).from_select(
        [
            star.DimAirplane.airplane_sk,
            star.DimAirplane.model,
            star.DimAirplane.registration_number,
            star.DimAirplane.fuel_consumption_per_hour,
            star.DimAirplane.maintenance_days,
        ],
        select_stmt,
    )
    print(insert_stmt)
    session.execute(insert_stmt)
    print("+++Dim airplane loaded+++")

    print("--- Dim pilot ---")
    select_stmt = sqlalchemy.select(
        whm.Pilot.pilot_sk,
        whm.Pilot.name,
        whm.Pilot.license_number,
    )
    insert_stmt = sqlalchemy.insert(star.DimPilot).from_select(
        [
            star.DimPilot.pilot_sk,
            star.DimPilot.name,
            star.DimPilot.license_number,
        ],
        select_stmt,
    )
    session.execute(insert_stmt)
    print(insert_stmt)
    print("+++Dim pilot loaded+++")
    print("--- Dim customer ---")
    select_stmt = sqlalchemy.select(
        whm.Customer.customer_sk,
        whm.Customer.full_name,
        whm.Customer.email,
        whm.Customer.frequent_flyer,
    )
    insert_stmt = sqlalchemy.insert(star.DimCustomer).from_select(
        [
            star.DimCustomer.customer_sk,
            star.DimCustomer.full_name,
            star.DimCustomer.email,
            star.DimCustomer.frequent_flyer,
        ],
        select_stmt,
    )
    session.execute(insert_stmt)
    print(insert_stmt)
    print("+++Dim customer loaded+++")

    print("--- Dim flight ---")
    select_stmt = sqlalchemy.select(
        whm.Flight.flight_sk,
        whm.Flight.flight_number,
        whm.Flight.departure_airport_sk,
        whm.Flight.arrival_airport_sk,
        whm.Flight.departure_time.label("departure_datetime"),
        whm.Flight.arrival_time.label("arrival_datetime"),
        whm.Flight.airplane_sk,
        whm.Flight.pilot_sk,
        whm.Flight.copilot_sk,
        whm.Flight.status,
        whm.Flight.is_ferry_flight,
    )
    insert_stmt = sqlalchemy.insert(star.DimFlight).from_select(
        [
            star.DimFlight.flight_sk,
            star.DimFlight.flight_number,
            star.DimFlight.departure_airport_sk,
            star.DimFlight.arrival_airport_sk,
            star.DimFlight.departure_datetime,
            star.DimFlight.arrival_datetime,
            star.DimFlight.airplane_sk,
            star.DimFlight.pilot_sk,
            star.DimFlight.copilot_sk,
            star.DimFlight.status,
            star.DimFlight.is_ferry_flight,
        ],
        select_stmt,
    )
    session.execute(insert_stmt)
    print(insert_stmt)
    print("+++Dim flight loaded+++")

    print("--- Dim date ---")
    #-- generate date dimension
    session.execute(sqlalchemy.text("""
        INSERT INTO star_schema.dim_date (date, day, month, year, weekday)
        SELECT
            date::date,
            EXTRACT(DAY FROM date) AS day,
            EXTRACT(MONTH FROM date) AS month,
            EXTRACT(YEAR FROM date) AS year,
            TO_CHAR(date, 'Day') AS weekday
        FROM generate_series(
            '1970-01-01'::date,
            '2099-12-31'::date,
            '1 day'::interval
        ) AS date(date)
    """))
    print("+++Dim date loaded+++")

    print("--- Fact flight ---")
    #-- generate fact flight
    select_stmt = sqlalchemy.select(
        whm.Flight.flight_sk,
        departure_date_table.date_sk.label("departure_date_sk"),
        arrival_date_table.date_sk.label("arrival_date_sk"),
        whm.Flight.delay_minutes,
        whm.Flight.estimated_flight_hours,
        whm.Flight.is_ferry_flight,
    ).join(
        departure_date_table,
        sqlalchemy.cast(whm.Flight.departure_time, sqlalchemy.Date) == departure_date_table.date,
    ).join(
        arrival_date_table,
        sqlalchemy.cast(whm.Flight.arrival_time, sqlalchemy.Date) == arrival_date_table.date,
    )

    insert_stmt = sqlalchemy.insert(star.FactFlight).from_select(
        [
            star.FactFlight.flight_sk,
            star.FactFlight.departure_date_sk,
            star.FactFlight.arrival_date_sk,
            star.FactFlight.delay_minutes,
            star.FactFlight.estimated_flight_hours,
            star.FactFlight.is_ferry_flight,
        ],
        select_stmt,
    )
    session.execute(insert_stmt)
    print(insert_stmt)
    print("+++Fact flight loaded+++")

    print("--- Fact booking ---")
    #-- generate fact booking
    select_stmt = select_map['fact_booking']

    insert_stmt = sqlalchemy.insert(star.FactBooking).from_select(
        [
            star.FactBooking.flight_booking_sk,
            star.FactBooking.flight_sk,
            star.FactBooking.customer_sk,
            star.FactBooking.seat_number,
        ],
        select_stmt,
    )
    session.execute(insert_stmt)
    print(insert_stmt)
    print("+++Fact booking loaded+++")

    print("--- Fact review ---")
    #-- generate fact review
    select_stmt = sqlalchemy.select(
        whm.AirlineReview.airline_review_sk,
        star.DimFlight.flight_sk,
        star.DimCustomer.customer_sk,
        whm.AirlineReview.seat_class,
        whm.AirlineReview.rating,
        whm.AirlineReview.recommended,
        whm.AirlineReview.seat_comfort,
        whm.AirlineReview.cabin_staff_service,
        whm.AirlineReview.food_and_beverages,
        whm.AirlineReview.inflight_entertainment,
        whm.AirlineReview.value_for_money,
        star.DimDate.date_sk.label("date_published_sk"),
    ).join(
        star.DimFlight,
        whm.AirlineReview.flight_sk == star.DimFlight.flight_sk,
    ).join(
        star.DimCustomer,
        whm.AirlineReview.customer_sk == star.DimCustomer.customer_sk,
    ).join(
        star.DimDate,
        sqlalchemy.cast(whm.AirlineReview.date_published, sqlalchemy.Date) == star.DimDate.date,
    )

    insert_stmt = sqlalchemy.insert(star.FactReview).from_select(
        [
            star.FactReview.airline_review_sk,  
            star.FactReview.flight_sk,
            star.FactReview.customer_sk,
            star.FactReview.seat_class,
            star.FactReview.rating,
            star.FactReview.recommended,
            star.FactReview.seat_comfort,
            star.FactReview.cabin_staff_service,
            star.FactReview.food_and_beverages,
            star.FactReview.inflight_entertainment,
            star.FactReview.value_for_money,
            star.FactReview.date_published_sk,
        ],
        select_stmt,
    )
    print(insert_stmt)
    session.execute(insert_stmt)
    session.commit()
    print("--- Star schema loaded ---")

if __name__ == "__main__":
    full_load_star_schema(1)
    # incremental_load_star_schema()