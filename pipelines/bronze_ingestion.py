"""Land API data, then declaratively ingest it into Bronze.

Run `land_api_payload()` from a Databricks notebook or job. Do not call it from
the `@dp.table` function: Lakeflow can evaluate table functions more than once.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

API_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/"
    "UniversityChapters_Public/FeatureServer/0/query"
)
FIXTURE_PATH = Path(__file__).parents[1] / "fixtures" / "synthetic_chapters.json"
LANDING_PATH_CONFIG = "university_chapters.landing_path"

LANDING_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("source_url", StringType(), False),
        StructField("ingested_at", StringType(), False),
        StructField("raw_payload", StringType(), False),
    ]
)

BRONZE_TABLE_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("source_url", StringType(), False),
        StructField("ingested_at", StringType(), False),
        StructField("raw_payload", StringType(), False),
    ]
)


def fetch_all_features() -> list[dict]:
    """Fetch every CA, OR, and WA feature from the public ArcGIS API."""
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
    """Write one JSON-lines file to a Unity Catalog Volume for this run."""
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
    return str(target)


@dp.table(
    name="bronze.university_chapters",
    comment="Raw University Chapters API features with ingestion metadata.",
    schema=BRONZE_TABLE_SCHEMA,
)
def university_chapters_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(LANDING_SCHEMA)
        .load(spark.conf.get(LANDING_PATH_CONFIG))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .select("run_id", "source_url", "ingested_at", "raw_payload")
    )
