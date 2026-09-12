"""Lakeflow Declarative Pipeline: Bronze -> Silver and Quarantine.

Set the pipeline catalog in Databricks. This code then uses:
  bronze.university_chapters
  silver.university_chapters
  quarantine.university_chapters
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

BRONZE_TABLE = "bronze.university_chapters"

RAW_FEATURE_SCHEMA = StructType(
    [
        StructField(
            "attributes",
            StructType(
                [
                    StructField("OBJECTID", StringType(), True),
                    StructField("ChapterID", StringType(), True),
                    StructField("University_Chapter", StringType(), True),
                    StructField("City", StringType(), True),
                    StructField("State", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "geometry",
            StructType(
                [
                    StructField("x", StringType(), True),
                    StructField("y", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


@dp.materialized_view(
    name="silver.university_chapters",
    comment="Clean university chapters. Invalid coordinates are excluded.",
    schema="""
      chapter_id STRING,
      chapter_name STRING,
      city STRING,
      state STRING,
      longitude DOUBLE,
      latitude DOUBLE,
      source_object_id STRING,
      dq_status STRING,
      dq_warnings ARRAY<STRING>,
      ingest_run_id STRING,
      ingested_at TIMESTAMP
    """,
)
def university_chapters_silver():
    raw = (
        spark.read.table(BRONZE_TABLE)
        .withColumn("feature", F.from_json("raw_payload", RAW_FEATURE_SCHEMA))
        .select(
            F.col("feature.attributes.ChapterID").alias("chapter_id"),
            F.col("feature.attributes.University_Chapter").alias("chapter_name"),
            F.trim(F.col("feature.attributes.City")).alias("city"),
            F.upper(F.trim(F.col("feature.attributes.State"))).alias("state"),
            F.col("feature.geometry.x").alias("longitude_raw"),
            F.col("feature.geometry.y").alias("latitude_raw"),
            F.col("feature.attributes.OBJECTID").alias("source_object_id"),
            "run_id",
            "ingested_at",
        )
        .withColumn("longitude", F.col("longitude_raw").cast("double"))
        .withColumn("latitude", F.col("latitude_raw").cast("double"))
        .where(F.col("state").isin("CA", "OR", "WA"))
    )

    return (
        raw.where(
            F.col("longitude").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").between(-180.0, 180.0)
            & F.col("latitude").between(-90.0, 90.0)
        )
        .withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy("chapter_id").orderBy(
                    F.col("source_object_id").cast("long").desc_nulls_last()
                )
            ),
        )
        .where(F.col("row_number") == 1)
        .withColumn(
            "dq_status",
            F.when(
                F.col("city").isNull()
                | (F.col("city") == "")
                | (F.upper(F.col("city")) == "UNKNOWN"),
                F.lit("WARNING"),
            ).otherwise(F.lit("OK")),
        )
        .withColumn(
            "dq_warnings",
            F.when(
                F.col("dq_status") == "WARNING",
                F.array(F.lit("MISSING_OR_UNKNOWN_CITY")),
            ).otherwise(F.array().cast("array<string>")),
        )
        .drop("longitude_raw", "latitude_raw", "row_number")
        .withColumnRenamed("run_id", "ingest_run_id")
        .select(
            "chapter_id",
            "chapter_name",
            "city",
            "state",
            "longitude",
            "latitude",
            "source_object_id",
            "dq_status",
            "dq_warnings",
            "ingest_run_id",
            "ingested_at",
        )
    )


@dp.materialized_view(
    name="quarantine.university_chapters",
    comment="Bronze rows with invalid longitude or latitude.",
    schema="""
      chapter_id STRING,
      state STRING,
      longitude_raw STRING,
      latitude_raw STRING,
      dq_reason STRING,
      raw_payload STRING,
      ingest_run_id STRING,
      ingested_at TIMESTAMP
    """,
)
def university_chapters_quarantine():
    raw = (
        spark.read.table(BRONZE_TABLE)
        .withColumn("feature", F.from_json("raw_payload", RAW_FEATURE_SCHEMA))
        .select(
            F.col("feature.attributes.ChapterID").alias("chapter_id"),
            F.upper(F.trim(F.col("feature.attributes.State"))).alias("state"),
            F.col("feature.geometry.x").alias("longitude_raw"),
            F.col("feature.geometry.y").alias("latitude_raw"),
            "raw_payload",
            F.col("run_id").alias("ingest_run_id"),
            "ingested_at",
        )
        .withColumn("longitude", F.col("longitude_raw").cast("double"))
        .withColumn("latitude", F.col("latitude_raw").cast("double"))
        .where(F.col("state").isin("CA", "OR", "WA"))
    )

    return (
        raw.where(
            F.col("longitude").isNull()
            | F.col("latitude").isNull()
            | ~F.col("longitude").between(-180.0, 180.0)
            | ~F.col("latitude").between(-90.0, 90.0)
        )
        .withColumn("dq_reason", F.lit("INVALID_COORDINATES"))
        .select(
            "chapter_id",
            "state",
            "longitude_raw",
            "latitude_raw",
            "dq_reason",
            "raw_payload",
            "ingest_run_id",
            "ingested_at",
        )
    )
