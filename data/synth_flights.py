from typing import Union, overload, override
import random
from bisect import bisect_right, insort
from collections import defaultdict
from datetime import datetime, timedelta
from faker import Faker
from model.reldb import Flight, FlightCabinCrew, FlightBooking
from model.common import FlightStatusEnum

# Constants
FLIGHT_DURATION = timedelta(hours=2, minutes=30)
TURNAROUND_TIME = timedelta(minutes=30)
MAX_DELAY = timedelta(hours=2)
CANCELLATION_PROBABILITY = 0.02
FERRY_FLIGHT_PROBABILITY = 0.1

# Simulated airport-specific delay probabilities (between 0 and 1)
def generate_airport_delay_probabilities(airport_ids: list[int]) -> dict[int, float]:
    return {airport_id: random.uniform(0.05, 0.25) for airport_id in airport_ids}

# Binary search based schedule checks
def is_available(schedule: list[tuple[datetime, datetime]], new_start: datetime, new_end: datetime) -> bool:
    idx = bisect_right(schedule, (new_start, new_end))
    if idx > 0 and schedule[idx - 1][1] > new_start:
        return False
    if idx < len(schedule) and schedule[idx][0] < new_end:
        return False
    return True

def assign_schedule(schedule: list[tuple[datetime, datetime]], new_start: datetime, new_end: datetime):
    insort(schedule, (new_start, new_end))

def pick_entity_list(candidates: list[int], 
                schedule_map: dict[int, list[tuple[datetime, datetime]]], 
                dep_time: datetime, 
                block_end: datetime) -> Union[int, None]:
    for eid in random.sample(candidates, random.randint(120, 160)):
        sched = schedule_map[eid]
        if is_available(sched, dep_time, block_end):
            assign_schedule(sched, dep_time, block_end)
            return eid
    return None


def pick_entity(candidate_max: int, 
                schedule_map: dict[int, list[tuple[datetime, datetime]]], 
                dep_time: datetime, 
                block_end: datetime) -> Union[int, None]:
    eid = random.randint(1, candidate_max)
    sched = schedule_map[eid]
    if is_available(sched, dep_time, block_end):
        assign_schedule(sched, dep_time, block_end)
        return eid
    return None

def flight_producer(
    fake: Faker,
    batch_offset:int,
    batch_size: int,
    pilot_ids: list[int],
    airport_ids: list[int], 
    airport_delay_probs: dict[int, float], 
    airplane_ids: list[int],
    pilot_schedule: dict[int, list[tuple[datetime, datetime]]],
    airplane_schedule: dict[int, list[tuple[datetime, datetime]]],
    airplane_location: dict[int, int],
) -> tuple[list[Flight]]:
    random.seed(batch_offset)

    results = []

    base_day = datetime(2024, 1, 1) + timedelta(days=batch_offset)

    for i in range(batch_size):
        base_dep_time = base_day + timedelta(minutes=15 * i)

        # Choose an airplane that can fly from its current location
        viable_airplanes = [
            a for a in airplane_ids if airplane_location[a] in airport_ids
        ]
        airplane = None
        origin = destination = None

        for a in random.sample(viable_airplanes, min(50, len(viable_airplanes))):
            origin_candidate = airplane_location[a]
            destination_candidate = random.choice([aid for aid in airport_ids if aid != origin_candidate])

            # Calculate potential times
            delay = timedelta()
            if random.random() < airport_delay_probs[origin_candidate]:
                delay = timedelta(minutes=random.randint(15, int(MAX_DELAY.total_seconds() / 60)))

            dep_time = base_dep_time + delay
            arr_time = dep_time + FLIGHT_DURATION
            block_end = arr_time + TURNAROUND_TIME

            # Check if airplane is available
            if is_available(airplane_schedule[a], dep_time, block_end):
                assign_schedule(airplane_schedule[a], dep_time, block_end)
                airplane = a
                origin = origin_candidate
                destination = destination_candidate
                airplane_location[a] = destination  # Update airplane's new location
                break

        if not airplane:
            # Could not find a viable airplane
            continue


        is_ferry = random.random() < FERRY_FLIGHT_PROBABILITY
        dep_time = base_dep_time + delay
        arr_time = dep_time + FLIGHT_DURATION
        block_end = arr_time + TURNAROUND_TIME

        # Check for cancellation
        if random.random() < CANCELLATION_PROBABILITY:
            results.append(Flight(
                flight_number=fake.bothify(text='??####'),
                status=FlightStatusEnum.CANCELLED,
                departure_time=dep_time,
                arrival_time=None,
                delay_minutes=int(delay.total_seconds() // 60),
                departure_airport_id=origin,
                arrival_airport_id=destination,
                pilot_id=None,
                copilot_id=None,
                airplane_id=airplane,
                is_ferry_flight=False,
                estimated_flight_hours=FLIGHT_DURATION.total_seconds() / 3600,
            ))
            continue

        pilot = pick_entity_list(pilot_ids, pilot_schedule, dep_time, block_end)
        copilot = pick_entity_list(pilot_ids, pilot_schedule, dep_time, block_end)

        results.append(Flight(
            flight_number=fake.bothify(text='??####'),
            status=FlightStatusEnum.SCHEDULED if delay == timedelta() else FlightStatusEnum.DELAYED,
            departure_time=dep_time,
            arrival_time=arr_time,
            delay_minutes=int(delay.total_seconds() // 60),
            departure_airport_id=origin,
            arrival_airport_id=destination,
            pilot_id=pilot,
            copilot_id=copilot,
            airplane_id=airplane,
            is_ferry_flight=is_ferry,
            estimated_flight_hours=FLIGHT_DURATION.total_seconds() / 3600,
        ))

    return results,


def flight_complement_producer(
    fake: Faker,
    batch_offset:int,
    batch_size: int,
    flight_ids: list[tuple[int, datetime, datetime]],
    crew_ids: list[int],
    max_customer_id: int,
    crew_schedule: dict[int, list[tuple[datetime, datetime]]],
    customer_schedule: dict[int, list[tuple[datetime, datetime]]],
):
    random.seed(batch_offset)

    batch_crew_links = []
    batch_bookings = []

    batch_flights = flight_ids[batch_offset * batch_size:(batch_offset + 1) * batch_size]
    for flight_id, dep_time, block_end in batch_flights:
        crew = [pick_entity_list(crew_ids, crew_schedule, dep_time, block_end) for _ in range(random.randint(2, 4))]
        passengers = [
            pick_entity(max_customer_id, customer_schedule, dep_time, block_end) for _ in range(random.randint(120, 160))
        ]

        for crew_id in crew:
            batch_crew_links.append(FlightCabinCrew(flight_id=flight_id, cabin_crew_id=crew_id))

        for seat_num, cust_id in enumerate(passengers, start=1):
            batch_bookings.append(FlightBooking(flight_id=flight_id, customer_id=cust_id, seat_number=str(seat_num)))

    return batch_crew_links, batch_bookings
