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
changed; the sink upserts them into PostgreSQL (realtime.page_editors_1h).

Run: uv run python analysis/stream_top_pages.py [--slide "5 minutes"]
         [--watermark "10 minutes"] [--starting-offsets latest]
         [--checkpoint state/checkpoints/top_pages_1h] [--progress-log FILE]
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from load_edits import edit_columns, filter_edits, get_spark, parse_kafka
from warehouse import PG, jdbc_append

WINDOW = "1 hour"
WINDOW_SECONDS = 3600
UNIT_SECONDS = {"second": 1, "minute": 60, "hour": 3600}


def seconds(interval: str) -> int:
    """'5 minutes' -> 300."""
    n, unit = interval.split()
    return int(n) * UNIT_SECONDS[unit.rstrip("s")]


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


DDL = """
CREATE SCHEMA IF NOT EXISTS realtime;
CREATE TABLE IF NOT EXISTS realtime.page_editors_1h (
    window_start timestamptz NOT NULL,
    window_end   timestamptz NOT NULL,
    wiki         text        NOT NULL,
    title        text        NOT NULL,
    editors      bigint      NOT NULL,
    edits        bigint      NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_end, wiki, title)
);
CREATE TABLE IF NOT EXISTS realtime.page_editors_1h_load (
    window_start bigint NOT NULL,
    window_end   bigint NOT NULL,
    wiki         text   NOT NULL,
    title        text   NOT NULL,
    editors      bigint NOT NULL,
    edits        bigint NOT NULL
);
"""

# Update mode emits each changed (window, page) with its full current counts,
# not a delta, so an upsert is idempotent: replaying a batch after a crash
# writes the same values again.
UPSERT = """
INSERT INTO realtime.page_editors_1h (window_start, window_end, wiki, title, editors, edits)
SELECT to_timestamp(window_start), to_timestamp(window_end), wiki, title, editors, edits
  FROM realtime.page_editors_1h_load
ON CONFLICT (window_end, wiki, title)
DO UPDATE SET editors = EXCLUDED.editors, edits = EXCLUDED.edits, updated_at = now();
TRUNCATE realtime.page_editors_1h_load;
-- Windows ending before the one shown are finished; keep the table small.
DELETE FROM realtime.page_editors_1h
 WHERE window_end < (SELECT max(window_end) FROM realtime.page_editors_1h)
                    - interval '%(window)s seconds' + interval '%(slide)s seconds';
"""

LATEST = """
SELECT window_start, window_end, count(*) FROM realtime.page_editors_1h
 WHERE window_end = (SELECT max(window_end) FROM realtime.page_editors_1h)
                    - interval '%(window)s seconds' + interval '%(slide)s seconds'
 GROUP BY 1, 2
"""


class PostgresSink:
    """foreachBatch sink: upsert (window end, page) -> counts into
    realtime.page_editors_1h.

    "The last hour" is the window that ends right after the newest edit. An
    edit at time t belongs to every window ending in (t, t + 1 hour], so the
    window with the largest end, [t, t + 1 hour), holds only the newest
    minutes; the one wanted ends at (largest end) - 1 hour + slide. The dbt
    view live_page_editors_1h selects it the same way.
    """

    def __init__(self, slide: str):
        self.params = {"window": WINDOW_SECONDS, "slide": seconds(slide)}
        with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE realtime.page_editors_1h_load")

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        # Unix seconds: the JDBC driver would read Spark timestamps in the
        # JVM's time zone, not the session's UTC.
        rows = batch.select(F.unix_timestamp("window.start").alias("window_start"),
                            F.unix_timestamp("window.end").alias("window_end"),
                            "wiki", "title", "editors", "edits")
        jdbc_append(rows, "realtime.page_editors_1h_load")
        with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
            cur.execute(UPSERT, self.params)
            cur.execute(LATEST, self.params)
            latest = cur.fetchone()
        if latest:
            start, end, pages = latest
            print(f"batch {batch_id}: last hour [{start:%Y-%m-%d %H:%M}, {end:%H:%M}) UTC, "
                  f"{pages:,} pages in realtime.page_editors_1h", flush=True)


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
    ap.add_argument("--starting-timestamp",
                    help='UTC "YYYY-MM-DD HH:MM:SS"; overrides --starting-offsets (first start only, '
                         'then the checkpoint decides), e.g. 1 hour ago to fill the window at once')
    ap.add_argument("--max-offsets", type=int, default=200_000,
                    help="Kafka messages per micro-batch (bounds a replay)")
    ap.add_argument("--checkpoint", default="state/checkpoints/top_pages_1h")
    ap.add_argument("--progress-log", help="append each batch's progress JSON to this file")
    args = ap.parse_args()

    spark = get_spark("stream-top-pages")
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "wikimedia.recentchange.raw"))
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets)
    )
    if args.starting_timestamp:
        start = datetime.strptime(args.starting_timestamp, "%Y-%m-%d %H:%M:%S")
        reader = reader.option("startingTimestamp",
                               str(int(start.replace(tzinfo=timezone.utc).timestamp() * 1000)))
    kafka = reader.load()
    counts = live_counts(edit_columns(filter_edits(parse_kafka(kafka))), args.slide, args.watermark)

    query = (
        counts.writeStream
        .outputMode("update")
        .foreachBatch(PostgresSink(args.slide))
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
