import sqlalchemy

class FlightStatusEnum(sqlalchemy.Enum):
    SCHEDULED = "scheduled"
    DELAYED = "delayed"
    CANCELLED = "cancelled"