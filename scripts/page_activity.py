"""Find pages with many consecutive edits in a JSONL sample written by read_recentchange.py.

Uses the filter rules from CLAUDE.md (see sample_stats.is_kept).
Prints counts only, never editor names.

Usage:
    python scripts/page_activity.py sample.jsonl [--top 15]
"""

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime

from sample_stats import is_kept

REVERT_PATTERN = re.compile(r"revert|undid|undo|rollback|\brv\b", re.IGNORECASE)
BURST_WINDOW_SECONDS = 10 * 60


def max_in_window(times: list[float], window: float) -> int:
    best, start = 0, 0
    for end, t in enumerate(times):
        while t - times[start] > window:
            start += 1
        best = max(best, end - start + 1)
    return best


def page_metrics(edits: list[dict]) -> dict:
    edits.sort(key=lambda e: e["meta"]["dt"])
    times = [datetime.fromisoformat(e["meta"]["dt"].replace("Z", "+00:00")).timestamp() for e in edits]
    users = [e.get("user") for e in edits]
    deltas = [
        e["length"].get("new", 0) - (e["length"].get("old") or 0)
        for e in edits
        if isinstance(e.get("length"), dict)
    ]
    nonzero = [d for d in deltas if d]
    return {
        "edits": len(edits),
        "editors": len(set(users)),
        "alternations": sum(1 for a, b in zip(users, users[1:]) if a != b),
        "span_min": (times[-1] - times[0]) / 60,
        "max_10min": max_in_window(times, BURST_WINDOW_SECONDS),
        "size_flips": sum(1 for a, b in zip(nonzero, nonzero[1:]) if (a > 0) != (b > 0)),
        "reverts": sum(1 for e in edits if REVERT_PATTERN.search(e.get("comment") or "")),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()

    pages = defaultdict(list)
    with open(args.path, encoding="utf-8") as f:
        for line in f:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if is_kept(event):
                pages[(event["wiki"], event["title"])].append(event)

    sizes = Counter(len(v) for v in pages.values())
    total_edits = sum(len(v) for v in pages.values())
    print(f"Pages: {len(pages)}  edits: {total_edits}")
    print("\n## edits per page")
    for label, low, high in [("1", 1, 1), ("2", 2, 2), ("3-4", 3, 4), ("5-9", 5, 9), ("10+", 10, 10**9)]:
        n = sum(c for size, c in sizes.items() if low <= size <= high)
        print(f"{label:>5} edits: {n:>6} pages ({n / len(pages):.1%})")

    metrics = {key: page_metrics(edits) for key, edits in pages.items()}
    columns = ["edits", "editors", "alternations", "span_min", "max_10min", "size_flips", "reverts"]

    def print_ranking(title: str, sort_key) -> None:
        print(f"\n## {title}")
        print(f"{'wiki':<8} {'title':<40} " + " ".join(f"{c:>12}" for c in columns))
        ranked = sorted(metrics.items(), key=lambda kv: sort_key(kv[1]), reverse=True)[: args.top]
        for (wiki, title), m in ranked:
            values = " ".join(f"{m[c]:>12.0f}" for c in columns)
            print(f"{wiki:<8} {title[:40]:<40} {values}")

    print_ranking("most edits", lambda m: (m["edits"], m["editors"]))
    print_ranking("most alternations between different editors", lambda m: (m["alternations"], m["reverts"]))


if __name__ == "__main__":
    main()
