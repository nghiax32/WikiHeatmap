"""Load human article edits from the raw archive into PostgreSQL (Step 8).

Spark applies the shared filter rules (load_edits.filter_edits, the same code
as the streaming query) and writes one row per event to raw.edits. Duplicates
(same event_id) are kept on purpose: dbt dedupes in stg_edits and tests
unique on fact_edits, so the test checks real work.

Idempotent per event-time range: Spark writes the range to raw.edits_load,
then one transaction deletes the range from raw.edits and inserts the new
rows. A rerun (e.g. after more of a day arrived in the archive) replaces the
range instead of adding to it, and a failed run changes nothing.

Event time travels as Unix seconds and becomes timestamptz in PostgreSQL:
the JDBC driver would interpret Spark timestamps in the JVM's time zone.

Run: uv run python analysis/load_postgres.py <path> <start> <end>   (UTC)
     e.g. s3a://wikiheatmap/raw/recentchange/ "2026-11-12 00:00:00" "2026-11-13 00:00:00"
"""
import sys
import time

import psycopg2
from pyspark.sql import functions as F

from load_edits import filter_edits, get_spark, in_window, read_raw
from warehouse import PG, jdbc_append

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.edits (
    event_id    text        NOT NULL,
    event_time  timestamptz NOT NULL,
    type        text,
    wiki        text,
    server_name text,
    title       text,
    user_name   text,
    loaded_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS edits_event_time_idx ON raw.edits (event_time);
CREATE TABLE IF NOT EXISTS raw.edits_load (
    event_id    text   NOT NULL,
    event_ts    bigint NOT NULL,
    type        text,
    wiki        text,
    server_name text,
    title       text,
    user_name   text
);
TRUNCATE raw.edits_load;
"""

SWAP = """
DELETE FROM raw.edits
 WHERE event_time >= %(start)s::timestamp AT TIME ZONE 'UTC'
   AND event_time <  %(end)s::timestamp AT TIME ZONE 'UTC';
INSERT INTO raw.edits (event_id, event_time, type, wiki, server_name, title, user_name)
SELECT event_id, to_timestamp(event_ts), type, wiki, server_name, title, user_name
  FROM raw.edits_load;
TRUNCATE raw.edits_load;
"""

if __name__ == "__main__":
    path, start, end = sys.argv[1], sys.argv[2], sys.argv[3]

    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(DDL)

    spark = get_spark("load-postgres")
    rows = filter_edits(in_window(read_raw(spark, path), start, end)).select(
        F.col("meta.id").alias("event_id"),
        F.col("timestamp").alias("event_ts"),
        "type", "wiki", "server_name", "title",
        F.col("user").alias("user_name"),
    )
    t0 = time.perf_counter()
    jdbc_append(rows, "raw.edits_load")
    spark.stop()

    # psycopg2's connection context manager commits on success, rolls back on error.
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(SWAP, {"start": start, "end": end})
        cur.execute("""SELECT count(*), count(DISTINCT event_id), min(event_time), max(event_time)
                         FROM raw.edits
                        WHERE event_time >= %(start)s::timestamp AT TIME ZONE 'UTC'
                          AND event_time <  %(end)s::timestamp AT TIME ZONE 'UTC'""",
                    {"start": start, "end": end})
        n, distinct, first, last = cur.fetchone()
    print(f"[{start}, {end}) UTC: {n:,} rows in raw.edits ({n - distinct:,} duplicates), "
          f"{first} .. {last}, {time.perf_counter() - t0:.0f} s")
