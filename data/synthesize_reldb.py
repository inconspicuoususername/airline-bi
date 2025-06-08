import math
import random
import pandas as pd
from data import synth_flights
from model import common
from model.reldb import Base, Pilot, CabinCrew, Airport, Customer, Flight, FlightBooking, FlightCabinCrew, Airplane
from faker import Faker
import database
import database.reldb as reldb
from data.synth_pipeline import PipelineConfig, run_pipeline, default_session_factory
from sqlalchemy import select, text
from multiprocessing.managers import SyncManager, DictProxy
from collections import defaultdict

class CustomMPManager(SyncManager):
    defaultdict = defaultdict

CustomMPManager.register('defaultdict', defaultdict, DictProxy)


def customers_producer(fake: Faker, batch_size: int, *args) -> tuple[list[Customer]]:
    return ([
        Customer(
            full_name=fake.name(),
            email=fake.email(),
            frequent_flyer=random.choice(['Yes', 'No'])
        )
        for _ in range(batch_size)
    ],)

def flights_producer(fake: Faker, batch_offset: int, batch_size: int, *args) -> tuple[list[Base]]:
    batch_flights = []

    mp_airplane_schedule, pilot_ids, airport_ids, p_ferry_flight = args

    for _ in range(batch_size):
        departure, arrival = random.sample(airport_ids, 2)
        pilot, copilot = random.sample(pilot_ids, 2)
        departure_date = fake.date_time_between(start_date='-1y', end_date='now')
        is_ferry = random.random() < p_ferry_flight
        estimated_hours = round(random.uniform(1.0, 10.0), 1)

        assigned_airplane = None
        for airplane_id, schedule in mp_airplane_schedule.items():
            if departure_date not in schedule:
                assigned_airplane = airplane_id
                schedule.append(departure_date)
                break
        if assigned_airplane is None:
            continue

        flight = Flight(
            flight_number=fake.bothify(text='??####'),
            departure_airport_id=departure,
            arrival_airport_id=arrival,
            departure_time=departure_date,
            pilot_id=pilot,
            copilot_id=copilot,
            airplane_id=assigned_airplane,
            is_ferry_flight=is_ferry,
            estimated_flight_hours=estimated_hours
        )
        batch_flights.append(flight)

    return batch_flights,


def flight_complement_producer(fake: Faker, batch_offset: int, batch_size: int, *args) -> tuple[list[Base], list[Base]]:
    batch_crew_links = []
    batch_bookings = []

    flight_ids, cabin_crew_ids, max_customer_id = args

    batch_flight_ids = flight_ids[batch_offset * batch_size:(batch_offset + 1) * batch_size]

    for flight_id in batch_flight_ids:
        assigned_crew = random.sample(cabin_crew_ids, 4)
        for crew_id in assigned_crew:
            batch_crew_links.append(FlightCabinCrew(
                flight_id=flight_id,
                cabin_crew_id=crew_id
            ))

        passengers = random.randint(120, 160)
        booked_customers = [random.randint(1, max_customer_id) for _ in range(passengers)]
        for seat_num, cust_id in enumerate(booked_customers, start=1):
            batch_bookings.append(FlightBooking(
                flight_id=flight_id,
                customer_id=cust_id,
                seat_number=str(seat_num)
            ))

    return batch_crew_links, batch_bookings


def synthesize_reldb(
        num_aircraft: int = 40,
        flights_per_aircraft_per_day: int = 5,
        days_per_year: int = 365,
        pax_per_flight: int = 150,
        num_airports: int = 100,
        unique_customer_percentage: float = 0.25,
        batch_size: int = 4000,
        p_ferry_flight: float = 0.05,
):
    print("+++++ Synthesizing database...")
    print("Configuration:")
    print(f"  - Number of aircraft: {num_aircraft}")
    print(f"  - Flights per aircraft per day: {flights_per_aircraft_per_day}")
    print(f"  - Days per year: {days_per_year}")
    print(f"  - Passengers per flight: {pax_per_flight}")
    print(f"  - Number of airports: {num_airports}")
    print(f"  - Unique customer percentage: {unique_customer_percentage}")
    print(f"  - Flight batch size: {batch_size}")
    print(f"  - Ferry flight probability: {p_ferry_flight}")

    # Annual estimates
    annual_flights = num_aircraft * flights_per_aircraft_per_day * days_per_year
    annual_passengers = annual_flights * pax_per_flight

    # Regulatory estimates (US FAA + EASA guidelines)
    # 1 flight = 2 pilots (pilot + co-pilot), assume max 1000 flight hours per year per pilot (realistic upper limit)
    pilot_hours_per_flight = 2.5  # avg short-medium haul
    max_hours_per_pilot_per_year = 900
    total_pilot_hours_needed = annual_flights * pilot_hours_per_flight
    num_pilots = math.ceil(total_pilot_hours_needed / max_hours_per_pilot_per_year)

    # Cabin crew: avg 4 crew members per flight (A320/737 typical config), similar working hours
    crew_hours_per_flight = 2.5
    max_hours_per_crew_per_year = 1000
    total_crew_hours_needed = annual_flights * crew_hours_per_flight * 4
    num_cabin_crew = math.ceil(total_crew_hours_needed / max_hours_per_crew_per_year)

    num_customers = math.ceil(annual_passengers * unique_customer_percentage)

    # Distribution preview
    distribution_summary = pd.DataFrame({
        "Table": [
            "pilots",
            "cabin_crew",
            "flights",
            "flight_bookings",
            "airports",
            "customers"
        ],
        "Estimated Rows": [
            num_pilots,
            num_cabin_crew,
            annual_flights,
            annual_passengers,
            num_airports,
            num_customers
        ]
    })

    # print dataframe to console
    print("Distribution Summary:")
    print(distribution_summary)

    database.wipe_schema(reldb.engine, reldb.metadata)
    database.ensure_schema(reldb.engine, reldb.metadata)
    session = database.get_session(reldb.engine)
    fake = Faker()

    # --- Data Generation ---
    print("Generating pilots...")
    for _ in range(num_pilots):
        session.add(Pilot(name=fake.name(), license_number=fake.uuid4()))

    print("Generating cabin crew...")
    for _ in range(num_cabin_crew):
        session.add(CabinCrew(name=fake.name(), employee_id=fake.uuid4()))

    print("Generating airports...")
    for _ in range(num_airports):
        session.add(Airport(
            code=fake.unique.bothify(text='???').upper(),
            name=fake.city() + " International",
            city=fake.city(),
            country=fake.country()
        ))

    print("Generating airplanes...")
    models = ["Airbus A320neo", "Boeing 737 MAX 8", "Airbus A321neo", "Boeing 787-9", "Airbus A350-900"]
    for _ in range(num_aircraft):
        session.add(Airplane(
            model=random.choice(models),
            registration_number=fake.unique.bothify(text='REG-####'),
            fuel_consumption_per_hour=round(random.uniform(2.5, 6.0), 2),
            maintenance_days=random.randint(10, 30)
        ))

    print("Generating customers...")
    # batching
    run_pipeline(
        config=PipelineConfig(
            num_rows=num_customers,
            batch_size=batch_size,
        ),
        producer_fn=customers_producer,
        consumer_session_factory=default_session_factory,
    )
    print("+++++ Base data inserted.")

    print("Loading reference data...")

    session.commit()
    pilot_ids = [p.id for p in session.query(Pilot).all()]
    airport_ids = [a.id for a in session.query(Airport).all()]
    airplane_ids = [a.id for a in session.query(Airplane).all()]
    # customer_ids = [c.id for c in session.query(Customer).all()]
    
    manager = CustomMPManager()
    manager.start()
    # mp_airplane_schedule = manager.dict({airplane_id: [] for airplane_id in airplane_ids})

    pilot_schedule = manager.defaultdict(list)
    airplane_schedule = manager.defaultdict(list)
    airplane_location = manager.dict({airplane_id: random.choice(airport_ids) for airplane_id in airplane_ids})
    

    airport_delay_probs = synth_flights.generate_airport_delay_probabilities(airport_ids)


    print("Generating flights...")
    run_pipeline(
        config=PipelineConfig(
            num_rows=annual_flights,
            batch_size=batch_size,
            producer_extra_args=(
                pilot_ids,
                airport_ids,
                airport_delay_probs,
                airplane_ids,
                pilot_schedule,
                airplane_schedule,
                airplane_location,
            ),
        ),
        producer_fn=synth_flights.flight_producer,
        consumer_session_factory=default_session_factory,
    )

    max_customer_id = session.execute(text("SELECT MAX(id) FROM airline.customers")).scalar()
    cabin_crew_ids = [c.id for c in session.query(CabinCrew).all()]
    flight_id_dep_arr = [(f.id, f.departure_time, f.arrival_time) for f in session.query(Flight).from_statement(select(Flight.id, Flight.departure_time, Flight.arrival_time).where(Flight.status == common.FlightStatusEnum.SCHEDULED))]
    crew_schedule = manager.defaultdict(list)
    customer_schedule = manager.defaultdict(list)

    print("Generating flight complements...")
    run_pipeline(
        config=PipelineConfig(
            num_rows=annual_flights,
            batch_size=batch_size // (annual_passengers // annual_flights),
            producer_extra_args=(
                flight_id_dep_arr,
                cabin_crew_ids,
                max_customer_id,
                crew_schedule,
                customer_schedule,
            ),
        ),
        producer_fn=synth_flights.flight_complement_producer,
        consumer_session_factory=default_session_factory,
    )



    # for i in range(0, annual_flights, batch_size):
    #     print(f"\r  -> Batch {i // batch_size + 1} / {annual_flights // batch_size + 1}: Generating {batch_size} flights", end="")
    #     batch_crew_links = []
    #     batch_bookings = []

    #     for _ in range(batch_size):
    #         departure, arrival = random.sample(airport_ids, 2)
    #         pilot, copilot = random.sample(pilot_ids, 2)
    #         departure_date = fake.date_between(start_date='-1y', end_date='today')
    #         is_ferry = random.random() < p_ferry_flight
    #         estimated_hours = round(random.uniform(1.0, 10.0), 1)

    #         assigned_airplane = None
    #         for airplane_id, schedule in airplane_schedule.items():
    #             if departure_date not in schedule:
    #                 assigned_airplane = airplane_id
    #                 schedule.append(departure_date)
    #                 break
    #         if assigned_airplane is None:
    #             continue

    #         flight = Flight(
    #             flight_number=fake.bothify(text='??####'),
    #             departure_airport_id=departure,
    #             arrival_airport_id=arrival,
    #             departure_time=departure_date,
    #             pilot_id=pilot,
    #             copilot_id=copilot,
    #             airplane_id=assigned_airplane,
    #             is_ferry_flight=is_ferry,
    #             estimated_flight_hours=estimated_hours
    #         )
    #         session.add(flight)
    #         session.flush()

    #         if not is_ferry:
    #             assigned_crew = random.sample(cabin_crew_ids, 4)
    #             for crew_id in assigned_crew:
    #                 batch_crew_links.append(FlightCabinCrew(
    #                     flight_id=flight.id,
    #                     cabin_crew_id=crew_id
    #                 ))

    #             passengers = random.randint(120, 160)
    #             booked_customers = random.sample(customer_ids, passengers)
    #             for seat_num, cust_id in enumerate(booked_customers, start=1):
    #                 batch_bookings.append(FlightBooking(
    #                     flight_id=flight.id,
    #                     customer_id=cust_id,
    #                     seat_number=str(seat_num)
    #                 ))

    #     session.bulk_save_objects(batch_crew_links)
    #     session.bulk_save_objects(batch_bookings)
    #     session.commit()

    print("+++++ Flights, airplane assignments, crew, and bookings created.")

if __name__ == "__main__":
    synthesize_reldb()

# if __name__ == "__main__":
#     session = get_airline_session()
#     flight_ids = [f.id for f in session.query(Flight).all()]
#     cabin_crew_ids = [c.id for c in session.query(CabinCrew).all()]
#     max_customer_id = session.execute(text("SELECT MAX(id) FROM airline.customers")).scalar()
#     print('starting profiling')
#     import cProfile
#     cp = cProfile.Profile()
#     cp.runcall(flight_complement_producer, Faker(), 0, 10000, flight_ids, cabin_crew_ids, max_customer_id)
#     print('profiling done')
#     cp.print_stats()
