"""SQLAlchemy ORM models for the crime data warehouse."""

from geoalchemy2 import Geometry
from sqlalchemy import Column, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class DimRegion(Base):
    __tablename__ = "dim_regions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    region_code = Column(String(10), unique=True, nullable=False)  # e.g. GM0363
    region_name = Column(String(200), nullable=False)
    geometry = Column(Geometry("MULTIPOLYGON", srid=4326), nullable=True)

    crimes = relationship("FactCrime", back_populates="region")


class DimCrimeType(Base):
    __tablename__ = "dim_crime_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    crime_code = Column(String(50), unique=True, nullable=False)
    crime_name = Column(String(300), nullable=False)

    crimes = relationship("FactCrime", back_populates="crime_type")


class DimPeriod(Base):
    __tablename__ = "dim_periods"

    id = Column(Integer, primary_key=True, autoincrement=True)
    period_code = Column(String(20), unique=True, nullable=False)  # e.g. 2024JJ00
    year = Column(Integer, nullable=False)

    crimes = relationship("FactCrime", back_populates="period")


class FactCrime(Base):
    __tablename__ = "fact_crimes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    region_id = Column(Integer, ForeignKey("dim_regions.id"), nullable=False)
    crime_type_id = Column(Integer, ForeignKey("dim_crime_types.id"), nullable=False)
    period_id = Column(Integer, ForeignKey("dim_periods.id"), nullable=False)
    registered_crimes = Column(Float, nullable=True)
    registered_crimes_per_1000 = Column(Float, nullable=True)

    region = relationship("DimRegion", back_populates="crimes")
    crime_type = relationship("DimCrimeType", back_populates="crimes")
    period = relationship("DimPeriod", back_populates="crimes")

    __table_args__ = (
        UniqueConstraint("region_id", "crime_type_id", "period_id", name="uq_crime_fact"),
    )
