"""Watermark experiment (Step 10): replay a range of Kafka through the live
aggregate (stream_top_pages.live_counts) once per watermark, and measure what
each watermark drops and what it costs in state.

The replay keeps the live micro-batches: the live query triggers every 30 s,
so each of its batches holds the messages that arrived in Kafka in those
30 s. Spark only moves the watermark between batches, so how much is dropped
depends on how arrivals are cut into batches, and replaying straight from
Kafka (maxOffsetsPerTrigger) would cut them differently. Instead:
1. read the range from Kafka once (batch), cut it into 30-second buckets of
   Kafka arrival time, one Parquet file per bucket;
2. per watermark, stream those files back one per micro-batch, oldest first
   (the file source orders files by modification time, set to the bucket's
   time).
Buckets with no arrival (producer stopped) give no batch; the live query
runs empty batches then, which do not move the watermark either.

Run: uv run python analysis/watermark_experiment.py <from> <to>   (Kafka arrival time, UTC)
         [--watermarks "1 second,10 seconds,1 minute,5 minutes,10 minutes"]
         [--slide "5 minutes"] [--trigger-seconds 30] [--work-dir DIR]
In compose: docker compose --profile jobs run --rm -e KAFKA_BOOTSTRAP=kafka:19092 \
            load-edits python watermark_experiment.py <from> <to>
"""
import argparse
import json
import os
import shutil
import tempfile
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import StringType, StructField, StructType

from lateness import epoch_ms
from load_edits import edit_columns, filter_edits, get_spark, parse_kafka
from stream_top_pages import live_counts

BATCH_SCHEMA = StructType([StructField("value", StringType())])


def read_kafka(spark: SparkSession, start: str, end: str) -> DataFrame:
    """Messages that arrived in Kafka in [start, end): value, arrival (Unix ms)."""
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "wikimedia.recentchange.raw"))
        .option("startingTimestamp", epoch_ms(start))
        .option("endingTimestamp", epoch_ms(end))
        .load()
        .select(F.col("value").cast("string").alias("value"),
                F.unix_millis("timestamp").alias("arrival_ms"))
    )


def write_batches(messages: DataFrame, work: Path, trigger_s: int) -> int:
    """One Parquet file per trigger interval of arrival time, in work/batches,
    with the bucket's time as modification time. Returns the number of files."""
    raw = work / "buckets"
    (messages.withColumn("bucket", F.floor(F.col("arrival_ms") / 1000 / trigger_s))
     .repartition("bucket")          # one task, hence one file, per bucket
     .select("value", "bucket")
     .write.mode("overwrite").partitionBy("bucket").parquet(str(raw)))
    out = work / "batches"
    out.mkdir()
    n = 0
    for d in raw.glob("bucket=*"):
        bucket = int(d.name.split("=")[1])
        for i, f in enumerate(sorted(d.glob("*.parquet"))):
            dst = out / f"{bucket:012d}-{i}.parquet"
            f.rename(dst)
            os.utime(dst, (bucket * trigger_s, bucket * trigger_s))
            n += 1
    shutil.rmtree(raw)
    return n


class Progress(StreamingQueryListener):
    """Keeps every batch's progress (query.recentProgress keeps only 100)."""

    def __init__(self):
        self.batches: list[dict] = []

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        self.batches.append(json.loads(event.progress.json))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def replay(spark: SparkSession, work: Path, watermark: str, slide: str,
           progress: Progress) -> list[dict]:
    """Stream work/batches through live_counts, one file per micro-batch."""
    progress.batches = []
    files = (spark.readStream.schema(BATCH_SCHEMA)
             .option("maxFilesPerTrigger", 1)
             .parquet(str(work / "batches")))
    # parse_kafka casts value to string; it already is one.
    counts = live_counts(edit_columns(filter_edits(parse_kafka(files))), slide, watermark)
    checkpoint = work / f"checkpoint-{watermark.replace(' ', '')}"
    query = (counts.writeStream.outputMode("update").format("noop")
             .option("checkpointLocation", str(checkpoint))
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()
    # Listener events arrive asynchronously: wait for the last batch's.
    last = query.lastProgress["batchId"] if query.lastProgress else -1
    for _ in range(100):
        if any(b["batchId"] == last for b in progress.batches):
            break
        time.sleep(0.1)
    shutil.rmtree(checkpoint)
    return [b for b in progress.batches if b["numInputRows"] > 0]


def summary(batches: list[dict]) -> dict:
    """Totals over the replay, per state operator."""
    ops: dict[str, dict] = {}
    for b in batches:
        for s in b["stateOperators"]:
            o = ops.setdefault(s["operatorName"], {"dropped": 0, "peak_rows": 0, "peak_mb": 0.0})
            o["dropped"] += s.get("numRowsDroppedByWatermark", 0)
            o["peak_rows"] = max(o["peak_rows"], s["numRowsTotal"])
            o["peak_mb"] = max(o["peak_mb"], s["memoryUsedBytes"] / 1e6)
    ms = sorted(b["durationMs"]["triggerExecution"] for b in batches)
    return {"batches": len(batches),
            "input": sum(b["numInputRows"] for b in batches),
            "ops": ops,
            "median_batch_ms": ms[len(ms) // 2] if ms else 0}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("start", help='Kafka arrival time, UTC "YYYY-MM-DD HH:MM:SS"')
    ap.add_argument("end")
    ap.add_argument("--watermarks", default="1 second,10 seconds,1 minute,5 minutes,10 minutes")
    ap.add_argument("--slide", default="5 minutes")
    ap.add_argument("--trigger-seconds", type=int, default=30,
                    help="trigger interval of the live query (stream_top_pages.py)")
    ap.add_argument("--work-dir", help="local directory for the batches (default: a temp dir)")
    args = ap.parse_args()

    spark = get_spark("watermark-experiment")
    # Speed only: few state partitions for a replay on one machine. The
    # watermark is global, so drops do not depend on it.
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    progress = Progress()
    spark.streams.addListener(progress)

    work = Path(args.work_dir or tempfile.mkdtemp(prefix="watermark-"))
    if work.exists() and any(work.iterdir()):
        raise SystemExit(f"{work} is not empty")
    work.mkdir(parents=True, exist_ok=True)

    messages = read_kafka(spark, args.start, args.end)
    n_files = write_batches(messages, work, args.trigger_seconds)
    batches = spark.read.schema(BATCH_SCHEMA).parquet(str(work / "batches"))
    edits = edit_columns(filter_edits(parse_kafka(batches)))
    n_messages, n_edits = batches.count(), edits.count()
    print(f"\nKafka arrival [{args.start}, {args.end}) UTC: {n_messages:,} messages, "
          f"{n_edits:,} edits, {n_files:,} micro-batches of {args.trigger_seconds} s")

    rows = []
    for w in [w.strip() for w in args.watermarks.split(",")]:
        s = summary(replay(spark, work, w, args.slide, progress))
        rows.append((w, s))
        print(f"W = {w}: {json.dumps(s)}", flush=True)

    print(f"\n{'watermark':<12} {'batches':>7} {'edits dropped':>14} {'window rows dropped':>20} "
          f"{'peak state rows':>16} {'peak state MB':>14} {'median batch ms':>16}")
    for w, s in rows:
        dedupe = sum(o["dropped"] for name, o in s["ops"].items() if name.startswith("dedupe"))
        agg = sum(o["dropped"] for name, o in s["ops"].items() if not name.startswith("dedupe"))
        peak_rows = sum(o["peak_rows"] for o in s["ops"].values())
        peak_mb = sum(o["peak_mb"] for o in s["ops"].values())
        print(f"{w:<12} {s['batches']:>7,} {dedupe:>8,} ({dedupe / n_edits:6.3%}) {agg:>20,} "
              f"{peak_rows:>16,} {peak_mb:>14,.0f} {s['median_batch_ms']:>16,}")
    shutil.rmtree(work)
    spark.stop()
