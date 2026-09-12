"""Lakeflow Declarative Pipeline: Silver -> Gold."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.university_chapters_v1",
    comment="Consumer-facing university chapters data product.",
    schema="""
      chapter_id STRING,
      chapter_name STRING,
      city STRING,
      state STRING,
      longitude DOUBLE,
      latitude DOUBLE,
      dq_status STRING,
      dq_warnings ARRAY<STRING>,
      ingest_run_id STRING,
      published_at TIMESTAMP
    """,
)
def university_chapters_gold():
    return (
        spark.read.table("silver.university_chapters")
        .where(F.col("dq_status").isin("OK", "WARNING"))
        .select(
            "chapter_id",
            "chapter_name",
            "city",
            "state",
            "longitude",
            "latitude",
            "dq_status",
            "dq_warnings",
            "ingest_run_id",
            F.current_timestamp().alias("published_at"),
        )
    )
