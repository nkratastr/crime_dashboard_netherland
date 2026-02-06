"""Download Dutch municipality boundary GeoJSON from PDOK."""

import json
import logging
from pathlib import Path

import requests
from shapely.geometry import mapping, shape

logger = logging.getLogger(__name__)

PDOK_BASE = (
    "https://api.pdok.nl/kadaster/bestuurlijkegebieden/ogc/v1"
    "/collections/gemeentegebied/items"
)
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def fetch_municipalities_geojson() -> dict:
    """Fetch all municipality boundaries from PDOK, handling pagination via 'next' links."""
    all_features = []
    url = f"{PDOK_BASE}?f=json&limit=100"

    while url:
        logger.info("Fetching PDOK page: %s", url)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        all_features.extend(features)
        logger.info("Got %d features (total so far: %d)", len(features), len(all_features))

        # Follow OGC 'next' link for pagination
        url = None
        for link in data.get("links", []):
            if link.get("rel") == "next":
                url = link["href"]
                break

    geojson = {
        "type": "FeatureCollection",
        "features": all_features,
    }
    logger.info("Fetched %d municipality boundaries total", len(all_features))
    return geojson


def simplify_geojson(geojson: dict, tolerance: float = 0.0001) -> dict:
    """Simplify polygon geometries to reduce file size while preserving shape."""
    simplified_features = []
    for feature in geojson["features"]:
        geom = shape(feature["geometry"])
        simple_geom = geom.simplify(tolerance, preserve_topology=True)
        feature_copy = {
            "type": "Feature",
            "properties": feature["properties"],
            "geometry": mapping(simple_geom),
        }
        simplified_features.append(feature_copy)

    result = {"type": "FeatureCollection", "features": simplified_features}
    logger.info("Simplified %d features (tolerance=%.4f)", len(simplified_features), tolerance)
    return result


def save_geojson(geojson: dict, filename: str = "municipalities.geojson") -> Path:
    """Save GeoJSON to the raw data landing zone."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    logger.info("Saved municipality GeoJSON to %s (%d features)", path, len(geojson["features"]))
    return path


def ingest_geo_data() -> Path:
    """Full geo ingestion: fetch → simplify → save."""
    geojson = fetch_municipalities_geojson()
    geojson = simplify_geojson(geojson)
    return save_geojson(geojson)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_geo_data()
