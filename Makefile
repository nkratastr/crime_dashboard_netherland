.PHONY: setup up down ingest pipeline dashboard test lint clean

setup:
	pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

db:
	docker compose up -d db

ingest:
	python -m src.ingestion.cbs_client
	python -m src.ingestion.geo_client

pipeline:
	python -m src.pipeline.run_pipeline

dashboard:
	streamlit run src/dashboard/app.py

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

clean:
	docker compose down -v
	rm -rf data/raw/*.parquet data/raw/*.geojson
