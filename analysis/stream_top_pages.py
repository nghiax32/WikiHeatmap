"""Live ranking from Kafka: distinct editors per page over a sliding 1-hour
event-time window (Step 7, streaming query 1: aggregate).

Same metric as the batch jobs (load_edits: schema, filters), with two
streaming differences:
- dedupe with dropDuplicatesWithinWatermark: state for an event_id is kept
  only until the watermark passes it, instead of forever;
- approx_count_distinct instead of countDistinct, which streaming rejects
  ("Distinct aggregations are not supported on streaming DataFrames").
  On 1-hour windows of sample.jsonl it was off on 1 page of 5,766.

Output mode update: each micro-batch emits only the (window, page) rows that
changed. Until Postgres exists (Step 9) the sink keeps them in a dict on the
driver, as the upsert table will, and prints the newest window's top pages.

Run: uv run python analysis/stream_top_pages.py [--slide "5 minutes"]
         [--watermark "10 minutes"] [--starting-offsets latest]
         [--checkpoint state/checkpoints/top_pages_1h] [--progress-log FILE]
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from load_edits import edit_columns, filter_edits, get_spark, parse_kafka

WINDOW = "1 hour"
WINDOW_SECONDS = 3600
UNIT_SECONDS = {"second": 1, "minute": 60, "hour": 3600}


def seconds(interval: str) -> int:
    """'5 minutes' -> 300."""
    n, unit = interval.split()
    return int(n) * UNIT_SECONDS[unit.rstrip("s")]


def utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")


def live_counts(edits: DataFrame, slide: str, watermark: str) -> DataFrame:
    """Distinct editors and edits per page per sliding window."""
    return (
        edits
        .withWatermark("event_time", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("event_time", WINDOW, slide), "wiki", "title")
        .agg(
            F.approx_count_distinct("user").alias("editors"),
            F.count("*").alias("edits"),
        )
    )


class LatestWindowTable:
    """Stand-in for the Postgres upsert table: (window end, page) -> counts.

    "The last hour" is the window that ends right after the newest edit. An
    edit at time t belongs to every window ending in (t, t + 1 hour], so the
    window with the largest end, [t, t + 1 hour), holds only the newest
    minutes; the one wanted ends at (largest end) - 1 hour + slide.
    """

    def __init__(self, slide: str, top: int):
        self.slide = seconds(slide)
        self.top = top
        self.rows: dict[tuple, tuple[int, int]] = {}
        self.target = 0

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        # Unix seconds: collect() would turn timestamps into naive datetimes
        # in the machine's time zone, not the session's UTC.
        rows = batch.select(F.unix_timestamp("window.end").alias("end"),
                            "wiki", "title", "editors", "edits").collect()
        for r in rows:
            self.rows[(r.end, r.wiki, r.title)] = (r.editors, r.edits)
        if rows:
            latest = max(r.end for r in rows)
            self.target = max(self.target, latest - WINDOW_SECONDS + self.slide)
        # Windows ending before the target are no longer shown; keep the table small.
        self.rows = {k: v for k, v in self.rows.items() if k[0] >= self.target}
        ranked = sorted(((k, v) for k, v in self.rows.items() if k[0] == self.target),
                        key=lambda kv: (-kv[1][0], -kv[1][1], kv[0][1], kv[0][2]))
        print(f"\nbatch {batch_id}: last hour [{utc(self.target - WINDOW_SECONDS)}, "
              f"{utc(self.target)}) UTC, {len(ranked):,} pages")
        for (_, wiki, title), (editors, edits) in ranked[: self.top]:
            print(f"  {editors:>3} editors {edits:>4} edits  {wiki:<10} {title}")


def progress_line(p: dict) -> str:
    state = p["stateOperators"]
    dropped = sum(s.get("numRowsDroppedByWatermark", 0) for s in state)
    rows = sum(s["numRowsTotal"] for s in state)
    mem = sum(s["memoryUsedBytes"] for s in state) / 1e6
    # How far the newest processed edit is behind the batch's wall-clock time.
    newest = p["eventTime"].get("max")
    lag = ""
    if newest:
        lag_s = (datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00"))
                 - datetime.fromisoformat(newest.replace("Z", "+00:00"))).total_seconds()
        lag = f" | event-time lag {lag_s:,.0f} s"
    return (f"[progress] batch {p['batchId']} input {p['numInputRows']:,} rows "
            f"({p.get('processedRowsPerSecond', 0):,.0f}/s) | watermark {p['eventTime'].get('watermark')} "
            f"| state {rows:,} rows, {mem:,.0f} MB | dropped by watermark {dropped:,}{lag}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--slide", default="5 minutes")
    ap.add_argument("--watermark", default="10 minutes")
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--max-offsets", type=int, default=200_000,
                    help="Kafka messages per micro-batch (bounds a replay)")
    ap.add_argument("--checkpoint", default="state/checkpoints/top_pages_1h")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--progress-log", help="append each batch's progress JSON to this file")
    args = ap.parse_args()

    spark = get_spark("stream-top-pages")
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "wikimedia.recentchange.raw"))
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )
    counts = live_counts(edit_columns(filter_edits(parse_kafka(kafka))), args.slide, args.watermark)

    query = (
        counts.writeStream
        .outputMode("update")
        .foreachBatch(LatestWindowTable(args.slide, args.top))
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )
    last = None
    while query.isActive:
        time.sleep(10)
        p = query.lastProgress
        if p and p["batchId"] != last:
            last = p["batchId"]
            print(progress_line(p), flush=True)
            if args.progress_log:
                with open(args.progress_log, "a") as f:
                    f.write(json.dumps(p, default=str) + "\n")  # UUIDs in progress
    query.awaitTermination()
