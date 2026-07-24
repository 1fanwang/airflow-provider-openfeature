"""Try progressive delivery in ~10 seconds. No Docker, no backend, no running Airflow.

Ramps a canary pool from 0% to 100% across 20 DAGs using the dependency-free FractionalProvider, the
deterministic bucketing the policy uses to decide each task's pool. Then flips the flag back to 0, the
kill switch. Every backend (flagd, GrowthBook, Unleash, ...) drives the same policy the same way.

    pip install airflow-provider-openfeature
    python examples/quickstart.py
"""

from __future__ import annotations

import os
import time

from openfeature import api
from openfeature.evaluation_context import EvaluationContext

from openfeature_airflow.providers.fractional import FractionalProvider, VariantFlag

FLAG = "airflow.task.pool"
DAGS = [f"dag_{i:02d}" for i in range(40)]
_DELAY = float(os.environ.get("QUICKSTART_DELAY", "0"))  # >0 only for recording an animated demo


def canary_cohort(percent: int) -> list[str]:
    """Set the flag to a percentage and return the DAGs the policy would place on the canary pool."""
    api.set_provider(FractionalProvider(
        variant_flags={FLAG: VariantFlag([("canary_pool", percent), ("default_pool", 100 - percent)])}))
    client = api.get_client()
    return [d for d in DAGS
            if client.get_string_value(FLAG, "default_pool", EvaluationContext(targeting_key=d)) == "canary_pool"]


def bar(n: int, total: int = 40) -> str:
    return "\u2588" * n + "\u2591" * (total - n)


def main() -> None:
    print("\n  airflow-provider-openfeature  \u00b7  progressive delivery, no backend needed\n")
    print(f"  ramping {FLAG} across {len(DAGS)} DAGs (deterministic, so it's reversible):\n")
    time.sleep(_DELAY)
    for percent in (0, 10, 25, 50, 75, 100):
        n = len(canary_cohort(percent))
        print(f"    flag = {percent:3d}%   {bar(n)}  {n:2d}/40 \u2192 canary_pool")
        time.sleep(_DELAY)
    print(f"\n  kill switch: set the flag back to 0%   {bar(len(canary_cohort(0)))}   instant rollback\n")


if __name__ == "__main__":
    main()
