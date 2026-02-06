-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Dimension: Regions (municipalities)
CREATE TABLE IF NOT EXISTS dim_regions (
    id SERIAL PRIMARY KEY,
    region_code VARCHAR(10) UNIQUE NOT NULL,
    region_name VARCHAR(200) NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 4326)
);

CREATE INDEX IF NOT EXISTS idx_dim_regions_code ON dim_regions(region_code);
CREATE INDEX IF NOT EXISTS idx_dim_regions_geom ON dim_regions USING GIST(geometry);

-- Dimension: Crime types
CREATE TABLE IF NOT EXISTS dim_crime_types (
    id SERIAL PRIMARY KEY,
    crime_code VARCHAR(50) UNIQUE NOT NULL,
    crime_name VARCHAR(300) NOT NULL
);

-- Dimension: Time periods
CREATE TABLE IF NOT EXISTS dim_periods (
    id SERIAL PRIMARY KEY,
    period_code VARCHAR(20) UNIQUE NOT NULL,
    year INTEGER NOT NULL
);

-- Fact: Crime records
CREATE TABLE IF NOT EXISTS fact_crimes (
    id SERIAL PRIMARY KEY,
    region_id INTEGER NOT NULL REFERENCES dim_regions(id),
    crime_type_id INTEGER NOT NULL REFERENCES dim_crime_types(id),
    period_id INTEGER NOT NULL REFERENCES dim_periods(id),
    registered_crimes DOUBLE PRECISION,
    registered_crimes_per_1000 DOUBLE PRECISION,
    UNIQUE(region_id, crime_type_id, period_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_crimes_region ON fact_crimes(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_crimes_period ON fact_crimes(period_id);
CREATE INDEX IF NOT EXISTS idx_fact_crimes_type ON fact_crimes(crime_type_id);
