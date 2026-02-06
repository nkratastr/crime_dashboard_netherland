"""Database connection helper."""

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

# Load .env from the project root regardless of working directory
_env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_env_path)


def get_database_url() -> str:
    user = os.getenv("POSTGRES_USER", "crime_user")
    password = os.getenv("POSTGRES_PASSWORD", "crime_password")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "crime_db")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


engine = create_engine(get_database_url())
SessionLocal = sessionmaker(bind=engine)


def get_session() -> Session:
    return SessionLocal()
