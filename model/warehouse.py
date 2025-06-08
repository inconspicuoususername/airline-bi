#!/usr/bin/env python3

"""
This file contains the database models for the warehouse database.
The warehouse is the src database for the star schema.
It contains data from the operational airline database, alongside the csv of customer reviews.
Warehouse uses SCD Type 2 for all tables.
"""


from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, MetaData, String
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy.dialects.postgresql import ENUM

from model import common
import constants


metadata = MetaData(schema=constants.WAREHOUSE_SCHEMA)

class Base(DeclarativeBase):
    metadata = metadata
    start_date: Mapped[DateTime] = mapped_column(DateTime)
    end_date: Mapped[DateTime] = mapped_column(DateTime)
    source_id: Mapped[int] = mapped_column(Integer)
    insert_id: Mapped[int] = mapped_column(Integer)
    update_id: Mapped[int] = mapped_column(Integer, nullable=True)

class Pilot(Base):
    __tablename__ = 'pilots'
    pilot_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    pilot_id: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String)
    license_number: Mapped[str] = mapped_column(String)

class CabinCrew(Base):
    __tablename__ = 'cabin_crew'
    cabin_crew_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    cabin_crew_id: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String)
    employee_id: Mapped[str] = mapped_column(String)

class Customer(Base):
    __tablename__ = 'customers'
    customer_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    customer_id: Mapped[int] = mapped_column(Integer)
    full_name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)
    frequent_flyer: Mapped[str] = mapped_column(String)

class Airport(Base):
    __tablename__ = 'airports'
    airport_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    airport_id: Mapped[int] = mapped_column(Integer)
    code: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    city: Mapped[str] = mapped_column(String)
    country: Mapped[str] = mapped_column(String)

class Airplane(Base):
    __tablename__ = 'airplanes'
    airplane_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    airplane_id: Mapped[int] = mapped_column(Integer)
    model: Mapped[str] = mapped_column(String)
    registration_number: Mapped[str] = mapped_column(String)
    fuel_consumption_per_hour: Mapped[float] = mapped_column(Float)
    maintenance_days: Mapped[int] = mapped_column(Integer)

class Flight(Base):
    __tablename__ = 'flights'
    flight_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_id: Mapped[int] = mapped_column(Integer)
    flight_number: Mapped[str] = mapped_column(String)
    departure_airport_sk: Mapped[int] = mapped_column(Integer, ForeignKey('airports.airport_sk'))
    departure_airport_id: Mapped[int] = mapped_column(Integer)
    arrival_airport_sk: Mapped[int] = mapped_column(Integer, ForeignKey('airports.airport_sk'))
    arrival_airport_id: Mapped[int] = mapped_column(Integer)
    departure_time: Mapped[DateTime] = mapped_column(DateTime)
    arrival_time: Mapped[DateTime] = mapped_column(DateTime)
    delay_minutes: Mapped[int] = mapped_column(Integer)
    status: Mapped[common.FlightStatusEnum] = mapped_column(ENUM('scheduled', 'delayed', 'cancelled', name="flight_status"))
    pilot_sk: Mapped[int] = mapped_column(Integer, ForeignKey('pilots.pilot_sk'))
    pilot_id: Mapped[int] = mapped_column(Integer)
    copilot_sk: Mapped[int] = mapped_column(Integer, ForeignKey('pilots.pilot_sk'))
    copilot_id: Mapped[int] = mapped_column(Integer)
    airplane_sk: Mapped[int] = mapped_column(Integer, ForeignKey('airplanes.airplane_sk'))
    is_ferry_flight: Mapped[bool] = mapped_column(Boolean)
    estimated_flight_hours: Mapped[float] = mapped_column(Float)

class FlightCabinCrew(Base):
    __tablename__ = 'flight_cabin_crew'
    flight_cabin_crew_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_cabin_crew_id: Mapped[int] = mapped_column(Integer)
    flight_sk: Mapped[int] = mapped_column(Integer, ForeignKey('flights.flight_sk'))
    flight_id: Mapped[int] = mapped_column(Integer)
    cabin_crew_sk: Mapped[int] = mapped_column(Integer, ForeignKey('cabin_crew.cabin_crew_sk'))
    cabin_crew_id: Mapped[int] = mapped_column(Integer)

class FlightBooking(Base):
    __tablename__ = 'flight_bookings'
    flight_booking_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_booking_id: Mapped[int] = mapped_column(Integer)
    flight_sk: Mapped[int] = mapped_column(Integer, ForeignKey('flights.flight_sk'))
    customer_sk: Mapped[int] = mapped_column(Integer, ForeignKey('customers.customer_sk'))
    seat_number: Mapped[str] = mapped_column(String)

class AirlineReview(Base):
    __tablename__ = 'airline_reviews'
    airline_review_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_sk: Mapped[int] = mapped_column(Integer, ForeignKey('flights.flight_sk'))
    flight_id: Mapped[int] = mapped_column(Integer)
    customer_sk: Mapped[int] = mapped_column(Integer, ForeignKey('customers.customer_sk'))
    customer_id: Mapped[int] = mapped_column(Integer)
    seat_class: Mapped[str] = mapped_column(String)
    content: Mapped[str] = mapped_column(String)
    rating: Mapped[float] = mapped_column(Float)
    recommended: Mapped[bool] = mapped_column(Boolean)
    seat_comfort: Mapped[int] = mapped_column(Integer)
    cabin_staff_service: Mapped[int] = mapped_column(Integer)
    food_and_beverages: Mapped[int] = mapped_column(Integer)
    inflight_entertainment: Mapped[int] = mapped_column(Integer)
    value_for_money: Mapped[int] = mapped_column(Integer)
    date_published: Mapped[DateTime] = mapped_column(DateTime)