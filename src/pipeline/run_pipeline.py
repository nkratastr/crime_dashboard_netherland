"""Main pipeline orchestration: ingest → validate → transform → load."""

import logging
import sys
from pathlib import Path

# Ensure project root is on the Python path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from sqlalchemy import text

from src.database.connection import engine
from src.database.models import Base
from src.ingestion.cbs_client import ingest_crime_data
from src.ingestion.geo_client import ingest_geo_data
from src.quality.checks import run_all_checks
from src.transformation.transform import transform_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def create_tables() -> None:
    """Create all tables using SQLAlchemy models."""
    Base.metadata.create_all(engine)
    logger.info("Database tables created/verified")


def load_to_db(tables: dict[str, pd.DataFrame]) -> None:
    """Load transformed DataFrames into PostgreSQL (truncate + insert)."""
    with engine.begin() as conn:
        # Truncate in correct order (fact first, then dimensions)
        conn.execute(text("TRUNCATE TABLE fact_crimes CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_regions CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_crime_types CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_periods CASCADE"))
        logger.info("Truncated all tables")

    # Load dimensions first, then fact
    # Reset index to create the 'id' column matching the dimension IDs used in fact table
    for table_name in ["dim_regions", "dim_crime_types", "dim_periods"]:
        df = tables[table_name].copy()
        df.index.name = "id"
        df.index = df.index + 1  # Start IDs at 1
        df.to_sql(table_name, engine, if_exists="append", index=True)
        logger.info("Loaded %d rows into %s", len(df), table_name)

    # Adjust fact table IDs (0-based index → 1-based DB IDs)
    fact = tables["fact_crimes"].copy()
    fact["region_id"] = fact["region_id"] + 1
    fact["crime_type_id"] = fact["crime_type_id"] + 1
    fact["period_id"] = fact["period_id"] + 1
    fact.to_sql("fact_crimes", engine, if_exists="append", index=False)
    logger.info("Loaded %d rows into fact_crimes", len(fact))


def run() -> None:
    """Execute the full pipeline."""
    logger.info("=== Starting crime data pipeline ===")

    # Step 1: Ingest
    logger.info("--- Step 1: Ingestion ---")
    crime_path = ingest_crime_data()
    geo_path = ingest_geo_data()
    logger.info("Ingestion complete: %s, %s", crime_path, geo_path)

    # Step 2: Transform
    logger.info("--- Step 2: Transformation ---")
    tables = transform_all(crime_path)

    # Step 3: Validate
    logger.info("--- Step 3: Quality checks ---")
    run_all_checks(tables)

    # Step 4: Load
    logger.info("--- Step 4: Load to database ---")
    create_tables()
    load_to_db(tables)

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)
