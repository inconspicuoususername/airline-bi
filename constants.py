
DATABASE_URL = "postgresql://davud:password@localhost:5432/airline_db"

AIRLINE_SCHEMA = "airline"
WAREHOUSE_SCHEMA = "warehouse"
CSV_STAGING_SCHEMA = "csv_staging"
STAR_SCHEMA = "star_schema"

WAREHOUSE_RELDB_SOURCE_ID = 1
WAREHOUSE_CSV_SOURCE_ID = 2

AIRLINE_CONSTANTS = {
    "aircraft_models": ["Airbus A320neo", "Boeing 737 MAX 8", "Airbus A321neo", "Boeing 787-9", "Airbus A350-900"],
    "aircraft_fuel_consumption_per_hour": {
        "Airbus A320neo": 2.5,
        "Boeing 737 MAX 8": 2.5,
        "Airbus A321neo": 2.5,
        "Boeing 787-9": 2.5,
        "Airbus A350-900": 2.5,
    },
    "aircraft_maintenance_days": {
        "Airbus A320neo": 10,
        "Boeing 737 MAX 8": 10,
        "Airbus A321neo": 10,
        "Boeing 787-9": 10,
        "Airbus A350-900": 10,
    },
}