"""One-off: convert the JSONL.gz archive of scripts/archive_raw.py to Parquet
in the layout of the streaming raw query (stream_raw.py), so every reader
deals with one format.

Each archive line is one Kafka message value, so spark.read.text gives the
same `value` column the streaming query writes. The Kafka coordinates were not
archived and stay null. date/hour come from the archive folders, which, like
the stream, are dated by the Kafka message timestamp.

Written to its own path, not into the streaming sink's: a reader of the sink
path only sees files listed in its _spark_metadata log, so files written there
by a batch job would be silently ignored. Mode overwrite: the input is frozen
(the archiver is stopped first), so a rerun rebuilds the same output.

Run: uv run python analysis/convert_archive.py [<src> <dst>]
"""
import sys
import time

from pyspark.sql import functions as F

from load_edits import get_spark

SRC = "s3a://wikiheatmap/raw/recentchange/"
DST = "s3a://wikiheatmap/raw-parquet/recentchange-jsonl/"

if __name__ == "__main__":
    src, dst = (sys.argv[1], sys.argv[2]) if len(sys.argv) == 3 else (SRC, DST)
    spark = get_spark("convert-archive")
    # Keep date=2026-11-24 / hour=03 as the strings the stream writes, not DateType / 3.
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    t0 = time.perf_counter()
    lines = spark.read.text(src)
    (lines.select(
        "value",
        F.lit(None).cast("int").alias("kafka_partition"),
        F.lit(None).cast("long").alias("kafka_offset"),
        F.lit(None).cast("timestamp").alias("kafka_timestamp"),
        "date", "hour",
    )
        .write.mode("overwrite").partitionBy("date", "hour").parquet(dst))

    n_in = spark.read.text(src).count()
    n_out = spark.read.parquet(dst).count()
    print(f"{src} -> {dst}: {n_in:,} lines, {n_out:,} rows "
          f"({'match' if n_in == n_out else 'MISMATCH'}), {time.perf_counter() - t0:.0f} s")
    spark.stop()
