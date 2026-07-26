"""Realistic revenue-rollup pipeline for the case study.

A nightly ETL aggregates raw orders into daily revenue per region. Two implementations of the rollup:

- ``rollup_v1`` -- the original: for each (region, date) group it rescans the shard. O(rows x groups).
- ``rollup_v2`` -- the rewrite: a single pass with dict accumulation. O(rows).

Both return identical numbers (revenue = completed amounts minus refunds), so v2 is a drop-in speedup.
The case study canaries v2 behind a feature flag, ramps it across regions, and checks that the
runtime drops while the totals stay identical.
"""

from __future__ import annotations

import csv
import random
from collections import defaultdict
from pathlib import Path

REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "apac-south", "apac-north",
           "latam", "mea", "canada", "brazil", "india", "anz"]
STATUSES = (["completed"] * 85) + (["refunded"] * 10) + (["cancelled"] * 5)


def generate_orders_csv(path: str, rows_per_region: int = 40_000, days: int = 30, seed: int = 7) -> str:
    """Write a realistic orders.csv: order_id, region, order_date, amount, status."""
    rnd = random.Random(seed)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "region", "order_date", "amount", "status"])
        oid = 0
        for region in REGIONS:
            base = rnd.uniform(40, 120)  # regional average order value
            for _ in range(rows_per_region):
                oid += 1
                day = rnd.randint(1, days)
                amount = round(max(1.0, rnd.gauss(base, base * 0.4)), 2)
                w.writerow([oid, region, f"2026-04-{day:02d}", amount, rnd.choice(STATUSES)])
    return str(p)


def load_region(path: str, region: str) -> list[dict]:
    with open(path) as f:
        return [r for r in csv.DictReader(f) if r["region"] == region]


def _revenue(rows: list[dict], region: str, date: str) -> float:
    total = 0.0
    for r in rows:
        if r["region"] == region and r["order_date"] == date:
            if r["status"] == "completed":
                total += float(r["amount"])
            elif r["status"] == "refunded":
                total -= float(r["amount"])
    return round(total, 2)


def rollup_v1(rows: list[dict]) -> dict[tuple[str, str], float]:
    """Original: rescan the shard for every (region, date) group. Correct but slow."""
    keys = {(r["region"], r["order_date"]) for r in rows}
    return {(region, date): _revenue(rows, region, date) for region, date in keys}


def rollup_v2(rows: list[dict]) -> dict[tuple[str, str], float]:
    """Rewrite: one pass, dict accumulation. Same numbers, far fewer operations."""
    out: dict[tuple[str, str], float] = defaultdict(float)
    for r in rows:
        key = (r["region"], r["order_date"])
        amt = float(r["amount"])
        if r["status"] == "completed":
            out[key] += amt
        elif r["status"] == "refunded":
            out[key] -= amt
    return {k: round(v, 2) for k, v in out.items()}


if __name__ == "__main__":  # quick self-check: v1 and v2 agree
    import os
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        path = generate_orders_csv(os.path.join(tmp, "orders.csv"), rows_per_region=2000, days=10)
        rows = load_region(path, "us-east")
    assert rollup_v1(rows) == rollup_v2(rows), "v1 and v2 disagree!"
    print(f"parity OK on {len(rows)} rows, {len(rollup_v2(rows))} daily buckets")
