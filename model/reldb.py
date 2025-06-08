#!/usr/bin/env python3

"""
This file contains the database models for the operational airline database.
"""

from sqlalchemy import (
    DateTime, Enum as SAEnum, Integer, String, ForeignKey, Float, Boolean, Index
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy import MetaData
import constants
from model import common

# Metadata is an sqlalchemy abstraction that contains info such as the pg schema name
# When reflecting tables into the database, the schema name is used to determine which schema to reflect the table into
metadata = MetaData(schema=constants.AIRLINE_SCHEMA)

class Base(DeclarativeBase):
    metadata = metadata

class Pilot(Base):
    __tablename__ = 'pilots'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    license_number: Mapped[str] = mapped_column(String)

class CabinCrew(Base):
    __tablename__ = 'cabin_crew'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    employee_id: Mapped[str] = mapped_column(String)

class Airport(Base):
    __tablename__ = 'airports'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    city: Mapped[str] = mapped_column(String)
    country: Mapped[str] = mapped_column(String)

class Customer(Base):
    __tablename__ = 'customers'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    full_name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)
    frequent_flyer: Mapped[str] = mapped_column(String)

class Airplane(Base):
    __tablename__ = 'airplanes'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model: Mapped[str] = mapped_column(String)
    registration_number: Mapped[str] = mapped_column(String, unique=True)
    fuel_consumption_per_hour: Mapped[float] = mapped_column(Float)
    maintenance_days: Mapped[int] = mapped_column(Integer)

    # "flight_index": batch_index * flights_per_batch + i,
    #         "status": "scheduled" if delay == timedelta() else "delayed",
    #         "departure": dep_time,
    #         "arrival": arr_time,
    #         "delay_minutes": int(delay.total_seconds() // 60),
    #         "origin": origin,
    #         "destination": destination,
    #         "pilot_id": pilot,
    #         "crew_ids": crew,
    #         "customer_ids": passengers,
    #         "ferry_flight": is_ferry,
    #         "airplane_id": airplane,

class Flight(Base):
    __tablename__ = 'flights'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_number: Mapped[str] = mapped_column(String)
    departure_airport_id: Mapped[int] = mapped_column(Integer, ForeignKey('airports.id'))
    arrival_airport_id: Mapped[int] = mapped_column(Integer, ForeignKey('airports.id'))
    departure_time: Mapped[DateTime] = mapped_column(DateTime)
    arrival_time: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    delay_minutes: Mapped[int] = mapped_column(Integer)
    status: Mapped[common.FlightStatusEnum] = mapped_column(ENUM('scheduled', 'delayed', 'cancelled', name="flight_status"))
    pilot_id: Mapped[int] = mapped_column(Integer, ForeignKey('pilots.id'), nullable=True)
    copilot_id: Mapped[int] = mapped_column(Integer, ForeignKey('pilots.id'), nullable=True)
    airplane_id: Mapped[int] = mapped_column(Integer, ForeignKey('airplanes.id'))
    is_ferry_flight: Mapped[bool] = mapped_column(Boolean, default=False)
    estimated_flight_hours: Mapped[float] = mapped_column(Float)

class FlightCabinCrew(Base):
    __tablename__ = 'flight_cabin_crew'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_id: Mapped[int] = mapped_column(Integer, ForeignKey('flights.id'))
    cabin_crew_id: Mapped[int] = mapped_column(Integer, ForeignKey('cabin_crew.id'))

class FlightBooking(Base):
    __tablename__ = 'flight_bookings'
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_id: Mapped[int] = mapped_column(Integer, ForeignKey('flights.id'))
    customer_id: Mapped[int] = mapped_column(Integer, ForeignKey('customers.id'))
    seat_number: Mapped[str] = mapped_column(String)


#indexes

idx_flight_id = Index('idx_flight_id', Flight.id)
idx_flight_number = Index('idx_flight_number', Flight.flight_number)
idx_flights_dep_arr = Index('idx_flights_dep_arr', Flight.departure_airport_id, Flight.arrival_airport_id)
idx_flights_pilot_copilot = Index('idx_flights_pilot_copilot', Flight.pilot_id, Flight.copilot_id)
idx_flights_dep_time = Index('idx_flights_dep_time', Flight.departure_time)
idx_flights_status = Index('idx_flights_status', Flight.status)

idx_flight_cabin_crew = Index('idx_flight_cabin_crew', FlightCabinCrew.flight_id, FlightCabinCrew.cabin_crew_id)

idx_flight_bookings = Index('idx_flight_bookings', FlightBooking.flight_id)
idx_flight_bookings_customer = Index('idx_flight_bookings_customer', FlightBooking.customer_id)
idx_flight_bookings_unique = Index('idx_flight_bookings_unique', FlightBooking.flight_id, FlightBooking.customer_id)