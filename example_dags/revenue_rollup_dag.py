"""Canary a faster aggregation in a revenue-rollup ETL, one region subset at a time.

The nightly job aggregates orders into daily revenue per region. A faster rewrite (`rollup_v2`) is
gated behind ``revenue_rollup.use_fast_agg``: each region reads the flag, runs the matching
implementation, and records its runtime and a revenue-parity guardrail so a wrong total shows up
immediately. Ramp the flag in your backend from 0% to 100% and revert with the dial, no redeploy. The
measured end-to-end version is in docs/case-study/.
"""

from __future__ import annotations

import datetime
import time
from collections import defaultdict

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator

REGIONS = ["us-east", "us-west", "eu-west", "apac-south", "latam", "mea"]


def _rollup_v1(rows):  # original: rescans the shard per (region, date) group
    keys = {(r["region"], r["date"]) for r in rows}
    return {k: round(sum(r["amount"] for r in rows if (r["region"], r["date"]) == k
                         and r["status"] == "completed"), 2) for k in keys}


def _rollup_v2(rows):  # rewrite: one pass, dict accumulation, same numbers
    out = defaultdict(float)
    for r in rows:
        if r["status"] == "completed":
            out[(r["region"], r["date"])] += r["amount"]
    return {k: round(v, 2) for k, v in out.items()}


def rollup_region(region: str, **_):
    """The task body: the flag picks the implementation; we measure runtime and check parity."""
    from openfeature_airflow.gate import flag_enabled
    from openfeature_airflow.measure import track_outcome

    rows = _extract(region)  # your real extract; a stub here so the example is self-contained
    use_v2 = flag_enabled("revenue_rollup.use_fast_agg", region, region=region)

    t = time.perf_counter()
    result = _rollup_v2(rows) if use_v2 else _rollup_v1(rows)
    elapsed_ms = (time.perf_counter() - t) * 1000

    if use_v2:  # guardrail: the rewrite must match the original to the cent
        assert result == _rollup_v1(rows), f"{region}: v2 revenue != v1"

    track_outcome("rollup_ms", region, value=round(elapsed_ms, 2),
                  variant="v2" if use_v2 else "v1", region=region)
    # XCom serializes the return as JSON; stringify the (region, date) keys (tuples aren't JSON keys)
    return {f"{r}|{d}": v for (r, d), v in result.items()}


def _extract(region: str):
    # stand-in for a real extract (a Spark read, a warehouse query, ...)
    return [{"region": region, "date": "2026-04-01", "amount": 100.0, "status": "completed"}]


with DAG(
    dag_id="revenue_rollup_example",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["openfeature", "example", "experiment"],
) as dag:
    for region in REGIONS:
        PythonOperator(task_id=f"rollup_{region.replace('-', '_')}",
                       python_callable=rollup_region, op_kwargs={"region": region})
