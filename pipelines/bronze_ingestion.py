"""Lakeflow declarative Bronze table for landed API files."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

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
