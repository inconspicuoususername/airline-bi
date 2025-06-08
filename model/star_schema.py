#!/usr/bin/env python3

"""
This file contains the database models for the star schema.
The star schema is the target database for the airline ETL process.
"""


import constants
from sqlalchemy import (
    Index, Integer, String, Float, Boolean, DateTime, ForeignKey, MetaData
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped

metadata = MetaData(schema=constants.STAR_SCHEMA)

class Base(DeclarativeBase):
    metadata = metadata

# === DIMENSION TABLES ===

class DimAirport(Base):
    __tablename__ = "dim_airport"
    airport_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    city: Mapped[str] = mapped_column(String)
    country: Mapped[str] = mapped_column(String)

class DimAirplane(Base):
    __tablename__ = "dim_airplane"
    airplane_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    model: Mapped[str] = mapped_column(String)
    registration_number: Mapped[str] = mapped_column(String)
    fuel_consumption_per_hour: Mapped[float] = mapped_column(Float)
    maintenance_days: Mapped[int] = mapped_column(Integer)

class DimPilot(Base):
    __tablename__ = "dim_pilot"
    pilot_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    license_number: Mapped[str] = mapped_column(String)

class DimCustomer(Base):
    __tablename__ = "dim_customer"
    customer_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    full_name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)
    frequent_flyer: Mapped[str] = mapped_column(String)

class DimFlight(Base):
    __tablename__ = "dim_flight"
    flight_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_number: Mapped[str] = mapped_column(String)
    departure_airport_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_airport.airport_sk"))
    arrival_airport_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_airport.airport_sk"))
    departure_datetime: Mapped[DateTime] = mapped_column(DateTime)
    arrival_datetime: Mapped[DateTime] = mapped_column(DateTime)
    airplane_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_airplane.airplane_sk"))
    pilot_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_pilot.pilot_sk"))
    copilot_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_pilot.pilot_sk"))
    status: Mapped[str] = mapped_column(String)
    is_ferry_flight: Mapped[bool] = mapped_column(Boolean)

class DimDate(Base):
    __tablename__ = "dim_date"
    date_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[DateTime] = mapped_column(DateTime)
    day: Mapped[int] = mapped_column(Integer)
    month: Mapped[int] = mapped_column(Integer)
    year: Mapped[int] = mapped_column(Integer)
    weekday: Mapped[str] = mapped_column(String)

# === FACT TABLES ===

class FactFlight(Base):
    __tablename__ = "fact_flight"
    flight_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_flight.flight_sk"), primary_key=True)
    departure_date_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_date.date_sk"))
    arrival_date_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_date.date_sk"))
    delay_minutes: Mapped[int] = mapped_column(Integer)
    estimated_flight_hours: Mapped[float] = mapped_column(Float)
    is_ferry_flight: Mapped[bool] = mapped_column(Boolean)

class FactBooking(Base):
    __tablename__ = "fact_booking"
    flight_booking_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_flight.flight_sk"))
    customer_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_customer.customer_sk"))
    seat_number: Mapped[str] = mapped_column(String)

class FactReview(Base):
    __tablename__ = "fact_review"
    airline_review_sk: Mapped[int] = mapped_column(Integer, primary_key=True)
    flight_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_flight.flight_sk"))
    customer_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_customer.customer_sk"))
    seat_class: Mapped[str] = mapped_column(String)
    rating: Mapped[float] = mapped_column(Float)
    recommended: Mapped[bool] = mapped_column(Boolean)
    seat_comfort: Mapped[int] = mapped_column(Integer)
    cabin_staff_service: Mapped[int] = mapped_column(Integer)
    food_and_beverages: Mapped[int] = mapped_column(Integer)
    inflight_entertainment: Mapped[int] = mapped_column(Integer)
    value_for_money: Mapped[int] = mapped_column(Integer)
    date_published_sk: Mapped[int] = mapped_column(ForeignKey("star_schema.dim_date.date_sk"))


#indexes

fact_booking_sk_index = Index("fact_booking_sk_index", FactBooking.flight_booking_sk)
fact_booking_flight_sk_index = Index("fact_booking_flight_sk_index", FactBooking.flight_sk, FactBooking.customer_sk)
