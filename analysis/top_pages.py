"""Rank pages by number of distinct editors within a time window.

Run: uv run python analysis/top_pages.py <path> <start> <end> <k>
     e.g. sample.jsonl "2026-11-09 07:00:00" "2026-11-09 08:00:00" 3   (UTC)
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from load_edits import get_spark, load_edits


def page_counts(edits: DataFrame) -> DataFrame:
    """Distinct editors and edits per page (unsorted; see by_rank).

    countDistinct ignores null users, so an edit with a hidden user counts
    towards edits but not editors.
    """
    return (
        edits
        .groupBy("wiki", "title")
        .agg(
            F.countDistinct("user").alias("editors"),
            F.count("*").alias("edits"),
        )
    )


def by_rank(counts: DataFrame) -> DataFrame:
    """Most editors first; wiki/title break ties so the order is deterministic.

    Kept out of page_counts: a global sort costs a shuffle and is wasted
    before top_k_per_wiki, which sorts within each wiki anyway.
    """
    return counts.orderBy(F.desc("editors"), F.desc("edits"), "wiki", "title")


def top_k_per_wiki(counts: DataFrame, k: int) -> DataFrame:
    """Keep the k highest-ranked pages of each wiki.

    row_number gives every page a distinct rank (ties broken by edits, then
    title), so each wiki returns at most k rows. rank/dense_rank would let
    ties exceed k, e.g. hundreds of pages tied at 1 editor.
    """
    w = Window.partitionBy("wiki").orderBy(
        F.desc("editors"), F.desc("edits"), "title")
    return (
        counts
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= k)
    )


if __name__ == "__main__":
    path, start, end, k = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    spark = get_spark("top-pages")

    # Cached: the queries below all reuse the same counts.
    counts = page_counts(load_edits(spark, path, start, end)).cache()

    print(f"window [{start}, {end}) UTC\n")
    by_rank(counts).show(20, truncate=False)

    counts.agg(
        F.count("*").alias("pages"),
        F.count_if(F.col("editors") >= 2).alias(">=2 editors"),
        F.count_if(F.col("editors") >= 3).alias(">=3 editors"),
        F.count_if(F.col("editors") >= 5).alias(">=5 editors"),
    ).show()

    # Top-k for the 5 wikis with the most edits in the window.
    top = top_k_per_wiki(counts, k)
    busiest = (counts.groupBy("wiki").agg(F.sum("edits").alias("wiki_edits"))
               .orderBy(F.desc("wiki_edits")).limit(5))
    print(f"top {k} pages per wiki (5 busiest wikis):")
    (top.join(busiest, "wiki")
        .orderBy(F.desc("wiki_edits"), "rank")
        .select("wiki", "rank", "title", "editors", "edits")
        .show(5 * k, truncate=False))
    print(f"top-{k} rows over all wikis: {top.count():,}")

    top.explain()
    spark.stop()
