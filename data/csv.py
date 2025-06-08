#! /usr/bin/env python3

"""
This file contains the code used to synthesize airline data. Namely, it will merge multiple
Kaggle datasets into a CSV, and mock the rest of the data as part of a Postgres database
using SQL alchemy and faker.
"""

from datetime import date, datetime
import csv
import random
import os
from faker import Faker
import sqlalchemy
from model.reldb import Flight
from model.common import FlightStatusEnum
from sqlalchemy import text, select
import database
import database.reldb as reldb

#final csv:
# Flight ID - randomly chosen from DB
# Customer ID - randomly chosen from DB
# Class - synthesized or pulled from reviews
# Content - review text
# Rating - overall rating
# Recommended - recommended
# Seat Comfort - seat comfort
# Cabin Staff Service - cabin staff service
# Food & Beverages - food & beverages
# Inflight Entertainment - inflight entertainment
# Value For Money - value for money
# Date Published - date published (synthesized)

#merge plan:
    #mocked tables:
        #- flight logs,
        #- booking (pulled from reviews)
        #- customers (pulled from reviews)
        #- employed pilots
        #- employed cabin crew
        #- employed ground staff
        #- maybe other staff or smth
        #- mocked flight routes

class CsvReview1:
    #Airlines,Name,Location,Date Published,Text Content,Seat Type,Seat Comfort,Cabin Staff Service,Food & Beverages,Inflight Entertainment,Value For Money,Recommended,Ground Service,Wifi & Connectivity
    def __init__(self, airlines: str, name: str, location: str, date_published: date, text_content: str, seat_type: str, seat_comfort: int, cabin_staff_service: int, food_and_beverages: int, inflight_entertainment: int, value_for_money: int, recommended: bool, ground_service: int, wifi_and_connectivity: int):
        self.airlines: str = airlines
        self.name: str = name
        self.location: str = location
        self.date_published: date = date_published
        #format: Trip Verified | content
        self.text_content: str = text_content
        self.seat_type: str = seat_type
        self.seat_comfort: int = seat_comfort
        self.cabin_staff_service: int = cabin_staff_service
        self.food_and_beverages: int = food_and_beverages
        self.inflight_entertainment: int = inflight_entertainment
        self.value_for_money: int = value_for_money
        self.recommended: bool = recommended
        self.ground_service: int = ground_service
        self.wifi_and_connectivity: int = wifi_and_connectivity


class CsvReview2:
    #Title,Name,Review Date,Airline,Verified,Reviews,Type of Traveller,Month Flown,Route,Class,Seat Comfort,Staff Service,Food & Beverages,Inflight Entertainment,Value For Money,Overall Rating,Recommended
    def __init__(self, title: str, name: str, review_date: datetime, airline: str, verified: bool, reviews: str, type_of_traveller: str, month_flown: str, route: str, seat_class: str, seat_comfort: int, staff_service: int, food_and_beverages: int, inflight_entertainment: int, value_for_money: int, overall_rating: int, recommended: bool):
        self.title: str = title
        self.name: str = name
        self.review_date: datetime = review_date
        self.airline: str = airline
        self.verified: bool = verified
        self.reviews: str = reviews
        self.type_of_traveller: str = type_of_traveller
        self.month_flown: str = month_flown
        self.route: str = route
        self.seat_class: str = seat_class
        self.seat_comfort: int = seat_comfort
        self.staff_service: int = staff_service 
        self.food_and_beverages: int = food_and_beverages
        self.inflight_entertainment: int = inflight_entertainment
        self.value_for_money: int = value_for_money
        self.overall_rating: int = overall_rating
        self.recommended: bool = recommended

class CsvReview3:
    #num,id,Gender,Customer Type,Age,Type of Travel,Class,Flight Distance,Inflight wifi service,Departure/Arrival time convenient,Ease of Online booking,Gate location,Food and drink,Online boarding,Seat comfort,Inflight entertainment,On-board service,Leg room service,Baggage handling,Checkin service,Inflight service,Cleanliness,Departure Delay in Minutes,Arrival Delay in Minutes,satisfaction
    def __init__(self, num: int, id: int, gender: str, customer_type: str, age: int, type_of_travel: str, seat_class: str, flight_distance: int, inflight_wifi_service: int, departure_arrival_time_convenient: int, ease_of_online_booking: int, gate_location: str, food_and_drink: int, online_boarding: int, seat_comfort: int, inflight_entertainment: int, on_board_service: int, leg_room_service: int, baggage_handling: int, checkin_service: int, inflight_service: int, cleanliness: int, departure_delay_in_minutes: int, arrival_delay_in_minutes: int, satisfaction: int):
        self.num: int = num
        self.id: int = id
        self.gender: str = gender
        self.customer_type: str = customer_type
        self.age: int = age
        self.type_of_travel: str = type_of_travel
        self.seat_class: str = seat_class
        self.flight_distance: int = flight_distance
        self.inflight_wifi_service: int = inflight_wifi_service
        self.departure_arrival_time_convenient: int = departure_arrival_time_convenient
        self.ease_of_online_booking: int = ease_of_online_booking
        self.gate_location: str = gate_location
        self.food_and_drink: int = food_and_drink
        self.online_boarding: int = online_boarding
        self.seat_comfort: int = seat_comfort
        self.inflight_entertainment: int = inflight_entertainment
        self.on_board_service: int = on_board_service
        self.leg_room_service: int = leg_room_service
        self.baggage_handling: int = baggage_handling
        self.checkin_service: int = checkin_service
        self.inflight_service: int = inflight_service
        self.cleanliness: int = cleanliness
        self.departure_delay_in_minutes: int = departure_delay_in_minutes
        self.arrival_delay_in_minutes: int = arrival_delay_in_minutes
        self.satisfaction: int = satisfaction

class AirlineReview:
    def __init__(self, flight_id: int, customer_id: int, seat_class: str, content: str, rating: float, recommended: bool, seat_comfort: int, cabin_staff_service: int, food_and_beverages: int, inflight_entertainment: int, value_for_money: int, date_published: datetime):
        self.flight_id: int = flight_id
        self.customer_id: int = customer_id
        self.seat_class: str = seat_class
        self.content: str = content
        self.rating: float = rating

        if not isinstance(recommended, bool):
            raise ValueError(f"Recommended must be a bool, got {type(recommended)}")
        self.recommended: bool = recommended
        self.seat_comfort: int = seat_comfort
        self.cabin_staff_service: int = cabin_staff_service
        self.food_and_beverages: int = food_and_beverages
        self.inflight_entertainment: int = inflight_entertainment
        self.value_for_money: int = value_for_money
        self.date_published: datetime = date_published

def parse_airline_review_1(fname: str) -> list[AirlineReview]:
    fake = Faker()
    cwd = os.getcwd()
    path = os.path.join(cwd, fname)
    print("path", path)
    output = []
    session = database.get_session(reldb.engine)

    flight_id_and_arrivals = [(f.id, f.arrival_time) for f in session.query(Flight).from_statement(select(Flight.id, Flight.arrival_time).where(Flight.status == FlightStatusEnum.SCHEDULED))]
    max_customer_id = session.execute(text("SELECT MAX(id) FROM airline.customers")).scalar()

    valid_flt_classes = ["Economy", "Premium Economy", "Business", "First"]

    review_fields = [
        'cabin_staff_service',
        'food_and_beverages',
        'inflight_entertainment',
        'value_for_money',
        'ground_service',
        'wifi_and_connectivity'
    ]

    with open(path, 'r') as f:
        reader = csv.reader(f)
        i =0
        next(reader)
        for line in reader:
            if line[0] == "":
                continue

            row = [x.strip() for x in line]

            print(i)
            i += 1

            fmtRow = CsvReview1(
                airlines=row[0],
                name=row[1],
                location=row[2],
                date_published=datetime.strptime(row[3], "%Y-%m-%d"),
                text_content=row[4],
                seat_type=row[5],
                seat_comfort=int(row[6] if row[6].strip().isdigit() else 0),
                cabin_staff_service=int(row[7] if row[7].strip().isdigit() else 0),
                food_and_beverages=int(row[8] if row[8].strip().isdigit() else 0),
                inflight_entertainment=int(row[9] if row[9].strip().isdigit() else 0),
                value_for_money=int(row[10] if row[10].strip().isdigit() else 0),
                recommended=row[11] == "True",
                ground_service=int(row[12] if row[12].strip().isdigit() else 0),
                wifi_and_connectivity=int(row[13] if row[13].strip().isdigit() else 0),
            )
            if max_customer_id is None:
                max_customer_id = 1
            customer_id = random.randint(1, max_customer_id)

            # calculate average of non-zero fields
            non_zero_fields = [getattr(fmtRow, field) for field in review_fields if getattr(fmtRow, field) != 0]
            average_rating = (sum(non_zero_fields) / len(non_zero_fields)) if len(non_zero_fields) > 0 else 0
            flight_id, arrival_time = random.choice(flight_id_and_arrivals)
            assert isinstance(arrival_time, datetime), f"Got {type(arrival_time)} instead of datetime"
            date_published: datetime = fake.date_time_between(start_date=arrival_time, end_date=datetime.now())

            text_content = fmtRow.text_content.split("|")
            out =""
            if len(text_content) != 2:
                out = fmtRow.text_content.strip()
            else:
                out = text_content[1].strip()

            output.append(AirlineReview(
                flight_id=flight_id,
                customer_id=customer_id,
                seat_class=random.choice(valid_flt_classes),
                content=out.replace("\n", " "),
                rating=average_rating,
                recommended=fmtRow.recommended,
                seat_comfort=fmtRow.seat_comfort,
                cabin_staff_service=fmtRow.cabin_staff_service,
                food_and_beverages=fmtRow.food_and_beverages,
                inflight_entertainment=fmtRow.inflight_entertainment,
                value_for_money=fmtRow.value_for_money,
                date_published=date_published,
            ))

    return output


def parse_our_reviews(fname: str) -> list[AirlineReview]:
    output = []
    with open(fname, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for line in reader:
            output.append(AirlineReview(
                flight_id=int(line[0]),
                customer_id=int(line[1]),
                seat_class=line[2],
                content=line[3],
                rating=float(line[4]),
                recommended=line[5] == "True",
                seat_comfort=int(line[6]),
                cabin_staff_service=int(line[7]),
                food_and_beverages=int(line[8]),
                inflight_entertainment=int(line[9]),
                value_for_money=int(line[10]),
                date_published=datetime.strptime(line[11], "%Y-%m-%d %H:%M:%S.%f"),
            ))

    return output

if __name__ == "__main__":
    output = parse_airline_review_1("data/input/airlines_review.csv")
    with open("data/output/reviews.csv", "w") as f:
        columns = output[0].__dict__.keys()
        w = csv.DictWriter(f, columns)
        w.writeheader()
        w.writerows([o.__dict__ for o in output])
