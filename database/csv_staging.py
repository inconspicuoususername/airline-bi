from sqlalchemy import MetaData, Integer, String, DateTime, Boolean, Float, create_engine
from sqlalchemy.orm import DeclarativeBase, mapped_column
from datetime import datetime
from constants import CSV_STAGING_SCHEMA, DATABASE_URL

engine = create_engine(DATABASE_URL)
metadata = MetaData(schema=CSV_STAGING_SCHEMA)


class Base(DeclarativeBase):
    metadata = metadata

class AirlineReview(Base):
    __tablename__ = "airline_reviews"

    review_TEMP_PK = mapped_column(Integer, primary_key=True)
    flight_id = mapped_column(Integer)
    customer_id = mapped_column(Integer)
    seat_class = mapped_column(String)
    content = mapped_column(String)
    rating = mapped_column(Float)
    recommended = mapped_column(Boolean)
    seat_comfort = mapped_column(Integer)
    cabin_staff_service = mapped_column(Integer)
    food_and_beverages = mapped_column(Integer)
    inflight_entertainment = mapped_column(Integer)
    value_for_money = mapped_column(Integer)
    date_published = mapped_column(DateTime)