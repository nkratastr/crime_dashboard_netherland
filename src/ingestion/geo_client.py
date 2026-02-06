"""Download Dutch municipality boundary GeoJSON from PDOK."""

import json
import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

PDOK_BASE = (
    "https://api.pdok.nl/kadaster/bestuurlijkegebieden/ogc/v1"
    "/collections/gemeentegebied/items"
)
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def fetch_municipalities_geojson() -> dict:
    """Fetch all municipality boundaries from PDOK, handling pagination."""
    all_features = []
    offset = 0
    limit = 100

    while True:
        url = f"{PDOK_BASE}?f=json&limit={limit}&offset={offset}"
        logger.info("Fetching PDOK page offset=%d ...", offset)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        logger.info("Got %d features (total so far: %d)", len(features), len(all_features))

        if len(features) < limit:
            break
        offset += limit

    geojson = {
        "type": "FeatureCollection",
        "features": all_features,
    }
    logger.info("Fetched %d municipality boundaries total", len(all_features))
    return geojson


def save_geojson(geojson: dict, filename: str = "municipalities.geojson") -> Path:
    """Save GeoJSON to the raw data landing zone."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    logger.info("Saved municipality GeoJSON to %s (%d features)", path, len(geojson["features"]))
    return path


def ingest_geo_data() -> Path:
    """Full geo ingestion: fetch → save."""
    geojson = fetch_municipalities_geojson()
    return save_geojson(geojson)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_geo_data()
