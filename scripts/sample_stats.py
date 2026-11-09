"""Print basic statistics for a JSONL sample written by read_recentchange.py.

Usage:
    python scripts/sample_stats.py sample.jsonl [--top 15]
"""

import argparse
import json
import statistics
from collections import Counter
from datetime import datetime, timedelta


def print_table(title: str, counts: Counter, bots: Counter, total: int, limit: int | None = None) -> None:
    print(f"\n## {title}")
    print(f"{'value':<28} {'events':>8} {'share':>7} {'bot':>7}")
    for value, n in counts.most_common(limit):
        print(f"{str(value):<28} {n:>8} {n / total:>7.1%} {bots[value] / n:>7.1%}")


def is_kept(event: dict) -> bool:
    """Filter rules from CLAUDE.md: Wikipedia articles edited or created by non-bots."""
    return (
        event["meta"].get("domain") != "canary"
        and event.get("server_name", "").endswith(".wikipedia.org")
        and event.get("type") in ("edit", "new")
        and event.get("bot") is False
        and event.get("namespace") == 0
    )


def full_minutes(first_dt: str, last_dt: str) -> list[str]:
    """Minutes between first and last event, excluding the two partial edge minutes."""
    start = datetime.fromisoformat(first_dt[:16]) + timedelta(minutes=1)
    end = datetime.fromisoformat(last_dt[:16])
    minutes = []
    while start < end:
        minutes.append(start.isoformat(timespec="minutes"))
        start += timedelta(minutes=1)
    return minutes


def rate_summary(per_minute: Counter, minutes: list[str], key=lambda m: m) -> str:
    values = sorted(per_minute[key(m)] for m in minutes)
    p95 = values[min(len(values) - 1, int(len(values) * 0.95))]
    return (f"min {values[0]:>5}  median {statistics.median(values):>7.1f}  "
            f"p95 {p95:>5}  max {values[-1]:>5}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--top", type=int, default=15, help="number of wikis to show")
    args = parser.parse_args()

    types, type_bots = Counter(), Counter()
    wikis, wiki_bots = Counter(), Counter()
    total = bot_total = 0
    first_dt = last_dt = None
    per_minute_all, per_minute_kept, per_wiki_minute_kept = Counter(), Counter(), Counter()
    kept_wikis = Counter()
    skipped_lines = 0

    with open(args.path, encoding="utf-8") as f:
        for line in f:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                skipped_lines += 1  # e.g. last line still being written
                continue
            is_bot = event.get("bot") is True
            event_type = event.get("type")
            wiki = event.get("wiki")
            dt = event["meta"]["dt"]

            total += 1
            bot_total += is_bot
            types[event_type] += 1
            type_bots[event_type] += is_bot
            wikis[wiki] += 1
            wiki_bots[wiki] += is_bot
            first_dt = dt if first_dt is None else min(first_dt, dt)
            last_dt = dt if last_dt is None else max(last_dt, dt)
            minute = dt[:16]
            per_minute_all[minute] += 1
            if is_kept(event):
                per_minute_kept[minute] += 1
                per_wiki_minute_kept[(wiki, minute)] += 1
                kept_wikis[wiki] += 1

    print(f"Events: {total}  ({first_dt} -> {last_dt})")
    print(f"Distinct wikis: {len(wikis)}")
    print(f"bot=true: {bot_total} ({bot_total / total:.1%})")
    print_table("type", types, type_bots, total)
    print_table(f"top {args.top} wiki", wikis, wiki_bots, total, args.top)

    minutes = full_minutes(first_dt, last_dt)
    print(f"\n## events per minute (event time, {len(minutes)} full minutes)")
    if skipped_lines:
        print(f"(skipped {skipped_lines} unreadable lines)")
    if not minutes:
        print("Not enough data.")
        return
    print(f"{'all events':<28} {rate_summary(per_minute_all, minutes)}")
    print(f"{'after filter rules':<28} {rate_summary(per_minute_kept, minutes)}")
    print(f"\n## per wiki after filter rules (top {args.top})")
    for wiki, _ in kept_wikis.most_common(args.top):
        summary = rate_summary(per_wiki_minute_kept, minutes, key=lambda m, w=wiki: (w, m))
        print(f"{wiki:<28} {summary}")


if __name__ == "__main__":
    main()
