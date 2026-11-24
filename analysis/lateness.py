"""How late do edits arrive in Kafka? (Step 7 experiment, reused in Step 10.)

Reads a range of the Kafka topic as a batch (by Kafka message timestamp, i.e.
arrival), applies the usual filters, and walks the edits in arrival order.
An edit's lateness is how far its event time is behind the newest event time
that arrived before it. A watermark of W drops an edit whose lateness is
above W (roughly: Spark advances the watermark once per micro-batch, from
the previous batch's max event time, so it drops a little less than this).

Run: uv run python analysis/lateness.py <from> <to>   (Kafka arrival time, UTC)
     e.g. "2026-11-23 11:00:00" "2026-11-23 11:30:00"
"""
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from load_edits import RAW_SCHEMA, filter_edits, get_spark

THRESHOLDS = [("1 s", 1), ("10 s", 10), ("1 min", 60), ("5 min", 300),
              ("10 min", 600), ("30 min", 1800), ("1 h", 3600)]


def epoch_ms(ts: str) -> str:
    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return str(int(dt.timestamp() * 1000))


if __name__ == "__main__":
    start, end = sys.argv[1], sys.argv[2]
    spark = get_spark("lateness")

    kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "wikimedia.recentchange.raw"))
        .option("startingTimestamp", epoch_ms(start))
        .option("endingTimestamp", epoch_ms(end))
        .load()
    )
    # Same parse and filters as everywhere else, keeping the Kafka coordinates.
    # Kafka's "timestamp" (arrival) is renamed: the event has its own "timestamp".
    parsed = (kafka.select(F.col("timestamp").alias("kafka_ts"), "partition", "offset",
                           F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("e"))
              .select("kafka_ts", "partition", "offset", "e.*"))
    edits = filter_edits(parsed).select("kafka_ts", "partition", "offset",
                                        F.col("timestamp").alias("t"))
    # Arrival order across partitions: Kafka timestamp, then partition/offset.
    # One window over all rows: fine for hours of edits, not for months.
    order = (Window.orderBy("kafka_ts", "partition", "offset")
             .rowsBetween(Window.unboundedPreceding, -1))
    late = (
        edits
        .withColumn("max_before", F.max("t").over(order))
        .withColumn("lateness", F.greatest(F.col("max_before") - F.col("t"), F.lit(0)))
        .withColumn("event_time", F.timestamp_seconds("t"))
        .cache()
    )

    n = late.count()
    print(f"\nKafka arrival [{start}, {end}) UTC: {n:,} edits")
    late.agg(F.min("event_time").alias("first event"), F.max("event_time").alias("last event"),
             F.expr("percentile(lateness, 0.99)").alias("p99 lateness s"),
             F.max("lateness").alias("max lateness s")).show(truncate=False)
    row = late.agg(*[F.count_if(F.col("lateness") > s).alias(name) for name, s in THRESHOLDS]).first()
    print("edits a watermark of W would drop (lateness > W):")
    for name, _ in THRESHOLDS:
        print(f"  W = {name:<7} {row[name]:>8,}  ({row[name] / n:.3%})" if n else f"  W = {name}: -")
    spark.stop()
