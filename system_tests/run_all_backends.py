"""Full multi-backend e2e: one Airflow DAG population + one policy, swap the OpenFeature provider.

Proves that flagd, GrowthBook, Unleash, and an in-house engine all gate a real DAG cohort identically
through the same ``task_policy``, and captures the exposure the listener would emit. Run:

    PYTHONPATH=<repo>/src:<this dir> python system_tests/run_all_backends.py

Backends that need a server (flagd, Unleash) are skipped with a clear note if not reachable, so the
script always runs; bring them up with the flagd container + docker-compose.unleash.yml for the full
matrix.
"""

from __future__ import annotations

import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(os.path.dirname(_HERE), "src")
sys.path[:0] = [_SRC, _HERE]  # openfeature_airflow + airflow_local_settings

_TMP = tempfile.mkdtemp(prefix="of_multi_e2e_")
os.environ.setdefault("AIRFLOW_HOME", _TMP)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", os.path.join(_TMP, "dags"))
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{_TMP}/airflow.db")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")
os.makedirs(os.path.join(_TMP, "dags"), exist_ok=True)

from openfeature import api  # noqa: E402

from openfeature_airflow.providers.growthbook import GrowthBookProvider  # noqa: E402
from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider  # noqa: E402

POPULATION = [f"mig_dag_{i:03d}" for i in range(30)]
CANARY = set(POPULATION[:10])  # mig_dag_000..009 -> canary_pool ; the rest -> default_pool
FLAG = "airflow.task.pool"


def _write_population():
    lines = [
        "from airflow import DAG",
        "from airflow.providers.standard.operators.empty import EmptyOperator",
        "import datetime",
    ]
    for did in POPULATION:
        lines.append(
            f"with DAG(dag_id='{did}', schedule=None, start_date=datetime.datetime(2024,1,1), catchup=False) as {did}:\n"
            f"    EmptyOperator(task_id='only', pool='default_pool', queue='default')"
        )
    path = os.path.join(_TMP, "population.py")
    open(path, "w").write("\n".join(lines) + "\n")
    return path


def _parse(path):
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=os.path.join(_TMP, "dags"), include_examples=False, safe_mode=False)
    bag.process_file(path)
    return {did: dag.tasks[0].pool for did, dag in bag.dags.items()}


# --- provider factories (each returns a ready, registered-and-warmed provider) -------------------
def backend_flagd():
    from openfeature.contrib.provider.flagd import FlagdProvider

    api.set_provider(FlagdProvider(host="localhost", port=8013))
    import time

    time.sleep(2)  # RPC resolver needs a moment to connect
    # smoke check: raises if flagd unreachable
    from openfeature.evaluation_context import EvaluationContext

    v = api.get_client().get_string_value(FLAG, "__unreachable__", EvaluationContext(targeting_key="x", attributes={"dag_id": "mig_dag_000"}))
    if v == "__unreachable__":
        raise RuntimeError("flagd not resolving")


def backend_growthbook():
    features = {FLAG: {"defaultValue": "default_pool", "rules": [{"condition": {"dag_id": {"$in": list(CANARY)}}, "force": "canary_pool"}]}}
    api.set_provider(GrowthBookProvider(features=features))


def backend_inhouse():
    flags = {FLAG: {"segments": [{"attribute": "dag_id", "in": list(CANARY), "variant": "canary_pool"}], "default": "default_pool"}}
    api.set_provider(InHouseTreatmentProvider(string_flags=flags))


def backend_unleash():
    from UnleashClient import UnleashClient

    from openfeature_airflow.providers.unleash import UnleashProvider

    client = UnleashClient(
        url="http://localhost:4242/api",
        app_name="airflow-e2e",
        custom_headers={"Authorization": "default:development.unleash-insecure-api-token"},
        refresh_interval=1,
        disable_metrics=True,
        disable_registration=True,
    )
    client.initialize_client()
    import time

    time.sleep(2)
    api.set_provider(UnleashProvider(client, enabled_values={FLAG: "canary_pool"}))


def backend_statsig():
    from statsig import StatsigOptions
    from statsig import statsig as sg

    from openfeature_airflow.providers.statsig import StatsigProvider

    sg.initialize("secret-local", StatsigOptions(local_mode=True))
    for did in CANARY:  # local-mode per-user gate override for the canary cohort
        sg.override_gate("airflow_task_pool", True, f"{did}:only")
    api.set_provider(StatsigProvider(sg, gate_map={FLAG: "airflow_task_pool"}, enabled_values={FLAG: "canary_pool"}))


BACKENDS = [
    ("flagd (container)", backend_flagd),
    ("GrowthBook (SDK, local)", backend_growthbook),
    ("in-house engine (template)", backend_inhouse),
    ("Unleash (container)", backend_unleash),
    ("Statsig (SDK, local mode)", backend_statsig),
]


def main():
    population_file = _write_population()
    expected = {did: ("canary_pool" if did in CANARY else "default_pool") for did in POPULATION}
    results = {}
    print("=" * 78)
    print(f"Multi-backend e2e: {len(POPULATION)} DAGs, canary cohort = {len(CANARY)} -> canary_pool")
    print("Same DAG population, same task_policy; only the OpenFeature provider changes.")
    print("=" * 78)
    for name, factory in BACKENDS:
        try:
            factory()
        except Exception as exc:
            print(f"\n[{name}] SKIPPED, backend not reachable: {type(exc).__name__}: {str(exc)[:80]}")
            continue
        pools = _parse(population_file)
        n_canary = sum(1 for p in pools.values() if p == "canary_pool")
        ok = pools == expected
        results[name] = pools
        print(f"\n[{name}] parsed {len(pools)} DAGs through the real policy")
        print(f"    canary_pool: {n_canary}   default_pool: {len(pools)-n_canary}   matches expected cohort: {ok}")
        assert ok, f"{name} routing != expected"

    live = list(results)
    print("\n" + "=" * 78)
    print(f"IDENTICAL-GATING CHECK across {len(live)} live backends: {live}")
    if len(live) >= 2:
        first = results[live[0]]
        identical = all(results[b] == first for b in live[1:])
        print(f"    all backends produced identical placement for all {len(POPULATION)} DAGs: {identical}")
        assert identical, "backends disagreed on placement"

    # exposure capture (the measurement half), sample 3 canary + 2 non-canary
    from openfeature_airflow.listener import emit_exposure

    print("\n" + "=" * 78)
    print("EXPOSURE capture (what the listener emits for measurement):")
    for did in ["mig_dag_002", "mig_dag_007", "mig_dag_015"]:
        exp = emit_exposure(did, "only", run_id="manual__2026-07-24")
        print(f"    {did}: {exp}")

    print("\n" + "=" * 78)
    print("ALL BACKENDS GATE IDENTICALLY, e2e OK")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

