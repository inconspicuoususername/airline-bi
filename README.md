# Business Intelligence ETL Project

This project implements a complete ETL (Extract, Transform, Load) pipeline for a business intelligence system. It demonstrates the process of extracting data from multiple sources (relational database and CSV files), transforming it into a data warehouse, and creating star schemas for analytical purposes. The project uses Prefect for workflow orchestration and Metabase for data visualization.

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Implementation Details](#implementation-details)
- [Data Flow](#data-flow)
- [Visualization](#visualization)
- [Development](#development)

## Project Overview

This project simulates a real-world business intelligence scenario where data from multiple sources needs to be integrated and transformed for analytical purposes. The system:

1. Generates synthetic data for a relational database and CSV files
2. Loads this data into a data warehouse
3. Creates star schemas for analytical queries
4. Provides visualization capabilities through Metabase

## Architecture

The project uses a modern data stack:

- **Data Sources**:
  - Relational Database (PostgreSQL)
  - CSV Files
- **ETL Orchestration**: Prefect
- **Data Warehouse**: PostgreSQL
- **Visualization**: Metabase
- **Object Storage**: MinIO (for storing CSV files)

## Prerequisites

- Docker and Docker Compose
- Python 3.12+
- Git

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd bi-project
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Start the services using Docker Compose:
```bash
docker compose up -d
```

This will start:
- PostgreSQL database
- Prefect server and worker
- Metabase
- MinIO

## Usage

### Accessing Services

- **Prefect UI**: http://localhost:4200
- **Metabase**: http://localhost:3000
- **MinIO Console**: http://localhost:9001

### Running the ETL Pipeline

1. Start the Prefect server, worker and Metabase frontend:
```bash
docker compose up -d
```

2. Deploy the flow:
```bash
python3 -m flows.initial.py
```

3. Either wait for the flow to be scheduled or run the flow directly from the **Prefect Server dashboard**

## Project Structure

```
.
├── data/               # Data generation and storage
├── database/          # Database models and connections
├── etl/              # ETL implementation
│   ├── warehouse.py  # Data warehouse operations
│   ├── star_schema.py # Star schema transformations
│   └── utils.py      # Utility functions
├── flows/            # Prefect workflow definitions
├── model/            # Data models
├── util/             # Utility functions
├── compose.yaml      # Docker Compose configuration
└── requirements.txt  # Python dependencies
```

## Implementation Details

### Data Generation

The project generates synthetic data for an airline operations system, including:

- **Pilots**: Pilot information, license numbers, and qualifications
- **Cabin Crew**: Staff details and employee IDs
- **Customers**: Passenger information, frequent flyer status
- **Airports**: Airport codes, names, locations
- **Airplanes**: Aircraft models, registration numbers, fuel consumption
- **Flights**: Flight schedules, routes, status
- **Flight Bookings**: Passenger bookings and seat assignments
- **Airline Reviews**: Customer feedback and ratings

### ETL Process

#### 1. Extract
- **Relational Database**:
  - Uses SQLAlchemy ORM for data extraction
  - Implements incremental loading with change data capture
  - Handles complex joins between related tables
  - Supports batch processing for large datasets

- **CSV Files**:
  - Pandas for efficient CSV reading
  - Handles various data formats and encodings
  - Supports incremental loading from CSV updates

#### 2. Transform
- **Data Warehouse Loading**:
  - Implements Type 2 Slowly Changing Dimensions (SCD)
  - Handles surrogate keys for dimension tables
  - Manages historical data tracking
  - Supports incremental updates

- **Data Quality**:
  - Validates data integrity
  - Handles missing values
  - Standardizes data formats
  - Enforces business rules

#### 3. Load
- **Warehouse Operations**:
  - Uses PostgreSQL for the data warehouse
  - Implements efficient bulk loading
  - Supports transaction management
  - Handles concurrent operations

- **Incremental Loading**:
  - Tracks changes using end_date timestamps
  - Supports partial updates
  - Maintains data consistency
  - Optimizes performance

### Star Schema Design

The project implements a comprehensive star schema for airline analytics:

#### Dimension Tables
1. **DimDate**:
   - Date attributes for temporal analysis
   - Supports various date hierarchies
   - Includes business day indicators

2. **DimAirport**:
   - Airport details
   - Geographic information
   - Operational metrics

3. **DimAirplane**:
   - Aircraft specifications
   - Maintenance information
   - Performance metrics

4. **DimPilot**:
   - Pilot qualifications
   - Experience metrics
   - Performance indicators

5. **DimCustomer**:
   - Customer demographics
   - Loyalty program status
   - Travel preferences

6. **DimFlight**:
   - Flight details
   - Route information
   - Schedule data

#### Fact Tables
1. **FactFlight**:
   - Flight performance metrics
   - Delay information
   - Operational statistics
   - Links to date, airport, and airplane dimensions

2. **FactBooking**:
   - Booking statistics
   - Revenue metrics
   - Seat utilization
   - Links to customer and flight dimensions

3. **FactReview**:
   - Customer satisfaction metrics
   - Service ratings
   - Feedback analysis
   - Links to customer, flight, and date dimensions

### Technical Implementation

#### Database Design
- Uses SQLAlchemy for ORM
- Implements proper indexing for performance
- Supports referential integrity
- Handles complex relationships

#### ETL Pipeline
- Prefect for workflow orchestration
- Modular design for maintainability
- Error handling and logging
- Monitoring and alerting

#### Data Quality
- Input validation
- Data type checking
- Business rule enforcement
- Error reporting

#### Performance Optimization
- Batch processing
- Parallel execution
- Efficient SQL queries
- Proper indexing

## Data Flow

1. Data is generated and stored in the relational database and CSV files
2. Prefect orchestrates the ETL process
3. Data is transformed and loaded into the warehouse
4. Star schemas are created for analytical queries
5. Metabase connects to the warehouse for visualization

## Visualization

Metabase is used for data visualization:

1. Access Metabase at http://localhost:3000
2. Set up a new database connection to the warehouse
3. Create dashboards and visualizations using the star schemas

## Development

### Adding New Features

1. Create new models in the `model/` directory
2. Add ETL logic in the `etl/` directory
3. Create new Prefect flows in the `flows/` directory
4. Update the documentation