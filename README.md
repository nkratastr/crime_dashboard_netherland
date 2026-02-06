# Netherlands Crime Dashboard

End-to-end data engineering project that ingests Dutch crime statistics from CBS Open Data, transforms them into a star schema, loads into PostgreSQL/PostGIS, and serves an interactive Streamlit dashboard with a choropleth heatmap.

## Architecture

```
CBS Open Data API ──> Ingestion ──> Raw Parquet
PDOK GeoJSON API ──> Ingestion ──> Raw GeoJSON
                                       │
                                 Transformation
                                       │
                                 Quality Checks
                                       │
                              PostgreSQL + PostGIS
                              (Star Schema: dims + fact)
                                       │
                              Streamlit Dashboard
                              (Heatmap + Charts)
```

## Tech Stack

| Layer           | Technology                        |
|-----------------|-----------------------------------|
| Ingestion       | Python, cbsodata, requests        |
| Storage         | PostgreSQL 16 + PostGIS 3.4       |
| Transformation  | pandas, SQLAlchemy                |
| Quality         | Custom validation framework       |
| Dashboard       | Streamlit, Folium, Plotly         |
| Infrastructure  | Docker, docker-compose            |

## Data Sources

- **Crime data**: [CBS dataset 83648NED](https://www.cbs.nl/nl-nl/cijfers/detail/83648NED) — Registered crime by type and region (2010–2024)
- **GeoJSON boundaries**: [PDOK Bestuurlijke Gebieden API](https://api.pdok.nl/kadaster/bestuurlijkegebieden/ogc/v1) — Dutch municipality boundaries

## Project Structure

```
├── docker-compose.yml          # PostgreSQL + PostGIS + Dashboard
├── Dockerfile                  # Dashboard container
├── Makefile                    # Common commands
├── src/
│   ├── ingestion/
│   │   ├── cbs_client.py       # CBS crime data ingestion
│   │   └── geo_client.py       # PDOK GeoJSON ingestion
│   ├── database/
│   │   ├── connection.py       # SQLAlchemy connection
│   │   ├── models.py           # ORM models (star schema)
│   │   └── migrations/         # SQL migration scripts
│   ├── transformation/
│   │   └── transform.py        # Clean, normalize, build dimensions + fact
│   ├── quality/
│   │   └── checks.py           # Data quality validations
│   ├── pipeline/
│   │   └── run_pipeline.py     # Orchestration: ingest → validate → transform → load
│   └── dashboard/
│       └── app.py              # Streamlit app with heatmap
├── tests/                      # Unit tests
└── data/raw/                   # Landing zone (gitignored)
```

## Database Schema (Star Schema)

- **dim_regions** — Municipality codes, names, PostGIS geometries
- **dim_crime_types** — Crime category codes and descriptions
- **dim_periods** — Year periods parsed from CBS codes
- **fact_crimes** — Registered crime counts and rates per 1,000 residents

## Quick Start

### Prerequisites

- Docker & docker-compose
- Python 3.10+

### Run with Docker

```bash
# Start all services (database + dashboard)
docker compose up -d

# Run the data pipeline to populate the database
docker compose exec dashboard python -m src.pipeline.run_pipeline

# Open the dashboard at http://localhost:8501
```

### Run Locally

```bash
# Install dependencies
make setup

# Start only the database
make db

# Run the full pipeline (ingest → validate → transform → load)
make pipeline

# Launch the dashboard
make dashboard
```

### Available Make Targets

| Command          | Description                          |
|------------------|--------------------------------------|
| `make setup`     | Install Python dependencies          |
| `make up`        | Start all Docker services            |
| `make down`      | Stop all Docker services             |
| `make db`        | Start only PostgreSQL                |
| `make ingest`    | Run data ingestion only              |
| `make pipeline`  | Run full ETL pipeline                |
| `make dashboard` | Launch Streamlit dashboard           |
| `make test`      | Run unit tests                       |
| `make lint`      | Run ruff linter                      |
| `make clean`     | Stop services and remove raw data    |

## Dashboard Features

- **Choropleth heatmap** — Crime rates by municipality on an interactive map
- **Year filter** — Select any year from 2010 to 2024
- **Crime type filter** — Filter by specific crime categories
- **Summary cards** — Total crimes, highest crime municipality, municipality count
- **Top 10 bar chart** — Municipalities with highest crime counts
- **National trend line** — Year-over-year crime trend across the Netherlands
- **Raw data table** — Expandable view of filtered data
