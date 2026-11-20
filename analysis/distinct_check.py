"""Two checks on counting distinct editors (Step 6, lesson 4).

1. Distinct counts are not additive: summing the editors of each sub-window
   (e.g. each 15 minutes) over-counts anyone who edits the page in more than
   one sub-window. This is why weekly/monthly rankings are recomputed from
   edit-level rows, not summed from daily rankings.
2. countDistinct (exact) vs approx_count_distinct (HyperLogLog): error per page
   and on the overall number of editors.

Run: uv run python analysis/distinct_check.py <path> <start> <end> <bucket>
     e.g. sample.jsonl "2026-11-09 07:00:00" "2026-11-09 08:00:00" "15 minutes"
"""
import sys
import time

from pyspark.sql import functions as F

from load_edits import get_spark, load_edits
from top_pages import page_counts

if __name__ == "__main__":
    path, start, end, bucket = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    spark = get_spark("distinct-check")

    # Reused by every query below, so read and dedupe once.
    edits = load_edits(spark, path, start, end).cache()
    print(f"window [{start}, {end}) UTC, {edits.count():,} edits\n")

    # --- 1. Non-additivity ------------------------------------------------
    whole = page_counts(edits).select("wiki", "title", "editors")
    summed = (
        edits
        .groupBy("wiki", "title", F.window("event_time", bucket).alias("bucket"))
        .agg(F.countDistinct("user").alias("bucket_editors"))
        .groupBy("wiki", "title")
        .agg(F.sum("bucket_editors").alias("summed_editors"),
             F.count("*").alias("buckets"))
    )
    cmp = whole.join(summed, ["wiki", "title"])

    print(f"1. editors of the whole window vs sum of '{bucket}' buckets")
    cmp.agg(
        F.sum("editors").alias("sum of exact editors"),
        F.sum("summed_editors").alias("sum of bucket sums"),
        F.count_if(F.col("summed_editors") > F.col("editors")).alias("pages over-counted"),
        F.count("*").alias("pages"),
    ).show()
    print("pages over-counted the most:")
    (cmp.withColumn("over", F.col("summed_editors") - F.col("editors"))
        .filter(F.col("over") > 0)
        .orderBy(F.desc("over"), F.desc("editors"), "wiki", "title")
        .show(10, truncate=False))

    # --- 2. Exact vs approximate -------------------------------------------
    print("2. countDistinct vs approx_count_distinct (default rsd = 0.05)")
    per_page = (
        edits.groupBy("wiki", "title")
        .agg(F.countDistinct("user").alias("exact"),
             F.approx_count_distinct("user").alias("approx"))
        .withColumn("err", F.col("approx") - F.col("exact"))
    )
    per_page.agg(
        F.count_if(F.col("err") != 0).alias("pages with error"),
        F.count("*").alias("pages"),
        F.max(F.abs("err")).alias("max abs error"),
    ).show()
    print("largest per-page errors:")
    (per_page.filter(F.col("err") != 0)
        .orderBy(F.desc(F.abs("err")), F.desc("exact"))
        .show(10, truncate=False))

    # Overall distinct editors: where HyperLogLog actually saves memory.
    for name, agg in [("countDistinct", F.countDistinct("user")),
                      ("approx_count_distinct", F.approx_count_distinct("user"))]:
        t0 = time.perf_counter()
        n = edits.agg(agg).first()[0]
        print(f"overall editors, {name:<22}: {n:,} ({time.perf_counter() - t0:.2f} s)")

    spark.stop()
