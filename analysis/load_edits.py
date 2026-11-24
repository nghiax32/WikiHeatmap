"""Load human article edits from raw recentchange events.

Applies the filter rules and dedupe agreed in CLAUDE.md and returns one row
per edit: event_id, event_time, wiki, title, user.

Reads a local file (sample.jsonl) or the raw archive on SeaweedFS
(s3a://wikiheatmap/raw/recentchange/).

Run: uv run python analysis/load_edits.py <path> [<start> <end>]   (UTC)
"""
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType)

# Only the fields we use. Fields missing from an event (e.g. title on canary
# events) become null; fields not listed here are ignored.
RAW_SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("id", StringType()),
        StructField("dt", StringType()),
        StructField("domain", StringType()),
    ])),
    StructField("timestamp", LongType()),
    StructField("type", StringType()),
    StructField("wiki", StringType()),
    StructField("server_name", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
])


# Must match the Hadoop version bundled with PySpark (pyspark/jars/hadoop-client-*.jar).
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.5.0"
# Must match the Spark and Scala versions of PySpark (pyspark/jars/spark-core_2.13-4.3.0.jar).
SPARK_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"


def get_spark(app: str) -> SparkSession:
    """Local SparkSession in UTC, able to read s3a:// paths on SeaweedFS
    and to read from Kafka."""
    spark = (
        SparkSession.builder.appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", f"{HADOOP_AWS},{SPARK_KAFKA}")
        .config("spark.hadoop.fs.s3a.endpoint",
                os.environ.get("S3_ENDPOINT", "http://localhost:8333"))
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ.get("S3_ACCESS_KEY", "wikiheatmap"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ.get("S3_SECRET_KEY", "wikiheatmap-secret"))
        # SeaweedFS: bucket in the path, not in the hostname; plain HTTP.
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        # The Hadoop 3.5 default stream (analytics accelerator) stalled on
        # SeaweedFS with "Connection pool shut down" on large .gz files.
        .config("spark.hadoop.fs.s3a.input.stream.type", "classic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(RAW_SCHEMA).json(path)


def filter_edits(raw: DataFrame) -> DataFrame:
    """Keep human edits to Wikipedia articles."""
    return raw.filter(
        (F.col("meta.domain") != "canary")
        & F.col("server_name").endswith(".wikipedia.org")
        & F.col("type").isin("edit", "new")
        & ~F.col("bot")
        & (F.col("namespace") == 0)
    )


def in_window(raw: DataFrame, start: str, end: str) -> DataFrame:
    """Keep events with start <= event time < end (half-open, so adjacent
    windows never count the same edit twice).

    Applied to raw events, before dedupe, so only the window is shuffled.
    On the archive, also skips date= folders that cannot hold the window:
    folders are dated by the time the event was sent to Kafka, which is never
    earlier than its event time (one day of margin), but can be much later
    (a replayed backlog), so only the lower bound can be pruned.
    """
    event_time = F.timestamp_seconds("timestamp")
    cond = ((event_time >= F.to_timestamp(F.lit(start)))
            & (event_time < F.to_timestamp(F.lit(end))))
    if "date" in raw.columns:
        cond &= F.col("date") >= F.date_sub(F.to_date(F.lit(start)), 1)
    return raw.filter(cond)


def parse_kafka(kafka: DataFrame) -> DataFrame:
    """Parse Kafka message values into the same columns as read_raw."""
    return (
        kafka
        .select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("e"))
        .select("e.*")
    )


def edit_columns(filtered: DataFrame) -> DataFrame:
    """Project to the edit columns (shared by batch and streaming)."""
    return filtered.select(
        F.col("meta.id").alias("event_id"),
        F.timestamp_seconds("timestamp").alias("event_time"),
        "wiki",
        "title",
        "user",
    )


def to_edits(filtered: DataFrame) -> DataFrame:
    """Project to the edit columns and drop resent events (same meta.id)."""
    # dropDuplicates only accepts top-level columns, hence the select first.
    return edit_columns(filtered).dropDuplicates(["event_id"])


def load_edits(spark: SparkSession, path: str,
               start: str | None = None, end: str | None = None) -> DataFrame:
    raw = read_raw(spark, path)
    if start is not None:
        raw = in_window(raw, start, end)
    return to_edits(filter_edits(raw))


if __name__ == "__main__":
    path = sys.argv[1]
    spark = get_spark("load-edits")

    raw = read_raw(spark, path)
    if len(sys.argv) == 4:
        raw = in_window(raw, sys.argv[2], sys.argv[3])
    filtered = filter_edits(raw)
    edits = to_edits(filtered)

    n_raw, n_filtered, n_edits = raw.count(), filtered.count(), edits.count()
    print(f"raw events      : {n_raw:,}")
    print(f"after filters   : {n_filtered:,} ({n_filtered / n_raw:.1%})")
    print(f"after dedupe    : {n_edits:,} (duplicates: {n_filtered - n_edits:,})")

    print("\nnull counts:")
    edits.select(
        [F.count_if(F.col(c).isNull()).alias(c) for c in edits.columns]
    ).show()

    edits.agg(F.min("event_time").alias("first"),
              F.max("event_time").alias("last")).show(truncate=False)
    edits.show(5, truncate=False)

    edits.explain()
    spark.stop()
