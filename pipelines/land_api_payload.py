"""Fetch the public API and land one JSON file per run in a UC Volume."""
from __future__ import annotations

import json
from pprint import pp
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/"
    "UniversityChapters_Public/FeatureServer/0/query"
)
FIXTURE_PATH = Path(__file__).parents[1] / "fixtures" / "synthetic_chapters.json"


def fetch_all_features() -> list[dict]:
    features: list[dict] = []
    offset = 0

    while True:
        response = requests.get(
            API_URL,
            params={
                "where": "State IN ('CA','OR','WA')",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": 1000,
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(f"ArcGIS API error: {payload['error']}")

        page = payload.get("features", [])
        features.extend(page)
        if not payload.get("exceededTransferLimit") or not page:
            return features
        offset += len(page)


def land_api_payload(landing_path: str, include_fixture: bool = True) -> str:
    """Write API data and optional DQ fixture rows to a JSON-lines file."""
    features = fetch_all_features()
    if include_fixture:
        features.extend(json.loads(FIXTURE_PATH.read_text())["features"])
    if not features:
        raise RuntimeError("Source API returned no features; no landing file was written.")

    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    ingested_at = datetime.now(timezone.utc).isoformat()
    destination = Path(landing_path)
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / f"university_chapters_{run_id}.json"

    with target.open("w") as stream:
        for feature in features:
            stream.write(
                json.dumps(
                    {
                        "run_id": run_id,
                        "source_url": API_URL,
                        "ingested_at": ingested_at,
                        "raw_payload": json.dumps(feature),
                    }
                )
                + "\n"
            )
    pp(features)
    # return str(target)


# if __name__ == "__main__":
#     land_api_payload("dbfs:/mnt/uc_volume/landing", include_fixture=True)