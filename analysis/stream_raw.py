"""Raw archive from Kafka to Parquet on SeaweedFS (Step 7, streaming query 2).

Separate from the aggregate query (own process, own checkpoint), so a failure
in the aggregation cannot cost the raw archive.

Lossless: each row keeps the full Kafka message value (raw recentchange JSON)
plus its Kafka coordinates, so any filter rule can be re-applied later. Rows
are partitioned by the Kafka message timestamp (time the producer sent it),
like scripts/archive_raw.py, so a late event never reopens an old folder.

The file sink is exactly-once: the checkpoint records which offsets each
micro-batch covers and _spark_metadata/ lists the files each batch committed;
readers of the output path (spark.read.parquet) ignore files not listed.

Run: uv run python analysis/stream_raw.py [--output s3a://wikiheatmap/raw-parquet/recentchange/]
         [--starting-offsets latest] [--trigger "5 minutes"]
         [--checkpoint state/checkpoints/raw_parquet]
"""
import argparse
import os

from pyspark.sql import functions as F

from load_edits import get_spark

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="s3a://wikiheatmap/raw-parquet/recentchange/")
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--max-offsets", type=int, default=500_000,
                    help="Kafka messages per micro-batch (bounds a replay)")
    ap.add_argument("--trigger", default="5 minutes",
                    help="longer = fewer, larger Parquet files")
    ap.add_argument("--checkpoint", default="state/checkpoints/raw_parquet")
    args = ap.parse_args()

    spark = get_spark("stream-raw")
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "wikimedia.recentchange.raw"))
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )
    raw = kafka.select(
        F.col("value").cast("string").alias("value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.date_format("timestamp", "yyyy-MM-dd").alias("date"),
        F.date_format("timestamp", "HH").alias("hour"),
    )
    query = (
        raw.writeStream
        .format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .partitionBy("date", "hour")
        .trigger(processingTime=args.trigger)
        .start()
    )
    while query.isActive:
        query.awaitTermination(60)
        p = query.lastProgress
        if p:
            print(f"[progress] batch {p['batchId']} input {p['numInputRows']:,} rows "
                  f"({p.get('processedRowsPerSecond', 0):,.0f}/s), "
                  f"offsets {p['sources'][0]['endOffset']}", flush=True)
