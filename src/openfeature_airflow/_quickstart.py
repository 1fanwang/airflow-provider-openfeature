"""Try progressive delivery in a few seconds. No Docker, no backend, no running Airflow scheduler.

Builds 40 real Airflow tasks, then runs the real placement policy (``apply_placement``) on them as a
flag ramps from 0% to 100%. Each task's actual ``pool`` attribute moves to the canary, reversibly. The
flag comes from the dependency-free FractionalProvider here; a real backend (flagd, GrowthBook, Unleash,
Statsig, ...) drives the same policy the same way.

Installed as the ``openfeature-airflow-quickstart`` console script, so it runs after a plain
``pip install airflow-provider-openfeature`` with no repo checkout.
"""

from __future__ import annotations

import datetime
import os
import time

os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from airflow import DAG  # noqa: E402

try:
    from airflow.providers.standard.operators.empty import EmptyOperator  # noqa: E402
except ImportError:  # pragma: no cover - Airflow 2.x import shim
    from airflow.operators.empty import EmptyOperator

from openfeature import api  # noqa: E402

from openfeature_airflow.policy import apply_placement  # noqa: E402
from openfeature_airflow.providers.fractional import FractionalProvider, VariantFlag  # noqa: E402

FLAG = "airflow.task.pool"
_DELAY = float(os.environ.get("QUICKSTART_DELAY", "0"))  # >0 only for recording an animated demo


def build_tasks():
    """40 real Airflow tasks, each starting on the default pool."""
    tasks = []
    for i in range(40):
        with DAG(f"dag_{i:02d}", schedule=None, start_date=datetime.datetime(2024, 1, 1)):
            tasks.append(EmptyOperator(task_id="run", pool="default_pool"))
    return tasks


def place(tasks, percent: int) -> int:
    """Set the flag to `percent`, run the real policy on every task, return how many moved to canary."""
    api.set_provider(FractionalProvider(
        variant_flags={FLAG: VariantFlag([("canary_pool", percent), ("default_pool", 100 - percent)])}))
    moved = 0
    for t in tasks:
        t.pool = "default_pool"       # reset
        apply_placement(t)            # the real cluster policy mutates t.pool from the flag
        moved += t.pool == "canary_pool"
    return moved


def bar(n: int, total: int = 40, width: int = 22) -> str:
    filled = round(n / total * width)
    return "\u2588" * filled + "\u2591" * (width - filled)


def main() -> None:
    tasks = build_tasks()
    probe = tasks[3]  # watch one real task's pool attribute
    print(f"\n  ramping {FLAG} across {len(tasks)} real Airflow tasks (no backend needed):\n")
    time.sleep(_DELAY)
    for percent in (0, 10, 25, 50, 75, 100):
        n = place(tasks, percent)
        print(f"    flag {percent:3d}%  {bar(n)}  {n:2d}/40 moved   dag_03.pool = {probe.pool}")
        time.sleep(_DELAY)
    place(tasks, 0)
    print(f"\n  kill switch \u2192 flag 0%   {bar(0)}   dag_03.pool = {probe.pool} (instant rollback)\n")


if __name__ == "__main__":  # pragma: no cover - console entry point
    main()
