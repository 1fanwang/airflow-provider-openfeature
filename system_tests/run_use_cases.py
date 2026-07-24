# Copyright note: Apache-2.0, see repo root LICENSE.
"""The four original use cases, gated live on real Airflow via flagd (+ backend-portability check).

- UC1  Airflow 2->3 migration       pool routing airflow_3x / airflow_2x        (policy)
- UC2  Kubernetes worker migration  queue routing kubernetes / default          (policy)
- UC3  KubernetesExecutor concurrent pods   code-path gate ramped by cluster     (gate)
- UC4  Disruption checkpointing     code-path gate for a task cohort             (gate)

Run:
    python system_tests/run_use_cases.py            # needs flagd on :8113 with use_case_flags.json
"""

from __future__ import annotations

import os
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.join(os.path.dirname(_HERE), "src")
sys.path[:0] = [_SRC, _HERE]

_TMP = tempfile.mkdtemp(prefix="of_usecases_")
os.environ.setdefault("AIRFLOW_HOME", _TMP)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", os.path.join(_TMP, "dags"))
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{_TMP}/airflow.db")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")
os.environ.setdefault("AIRFLOW__OPENFEATURE__ENABLE_POLICY", "True")  # arm the entrypoint policy
os.makedirs(os.path.join(_TMP, "dags"), exist_ok=True)

import time  # noqa: E402

from openfeature import api  # noqa: E402
from openfeature.contrib.provider.flagd import FlagdProvider  # noqa: E402

from openfeature_airflow.gate import flag_enabled  # noqa: E402

POPULATION = [f"dag_{i:03d}" for i in range(30)]
MIG_SET = set(POPULATION[:15])          # UC1: 15/30 migrate to 3.x
K8S_WORKER_SET = set(POPULATION[:10])          # UC2: 10/30 route to the kubernetes queue
CKPT_SET = set(POPULATION[20:])         # UC4: 10/30 get resumable checkpointing
CLUSTERS = [f"cluster-{i:02d}" for i in range(20)]
CLUSTER_SET = set(CLUSTERS[:5])         # UC3: 5/20 clusters get concurrent pod creation


def _write_population():
    lines = [
        "from airflow import DAG",
        "from airflow.providers.standard.operators.empty import EmptyOperator",
        "import datetime",
    ]
    for did in POPULATION:
        lines.append(
            f"with DAG(dag_id='{did}', schedule=None, start_date=datetime.datetime(2024,1,1), catchup=False) as {did}:\n"
            f"    EmptyOperator(task_id='only', pool='airflow_2x', queue='default')"
        )
    path = os.path.join(_TMP, "population.py")
    open(path, "w").write("\n".join(lines) + "\n")
    return path


def _parse(path):
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=os.path.join(_TMP, "dags"), include_examples=False, safe_mode=False)
    bag.process_file(path)
    return {did: (dag.tasks[0].pool, dag.tasks[0].queue) for did, dag in bag.dags.items()}


def main():
    api.set_provider(FlagdProvider(host="localhost", port=8113))
    time.sleep(2)
    print("=" * 78)
    print("Original use cases, gated live on real Airflow via flagd")
    print("=" * 78)

    placements = _parse(_write_population())

    # UC1 — Airflow 2->3 migration (pool)
    mig_ok = all(
        (placements[d][0] == ("airflow_3x" if d in MIG_SET else "airflow_2x")) for d in POPULATION
    )
    n3x = sum(1 for d in POPULATION if placements[d][0] == "airflow_3x")
    print(f"\nUC1  2->3 migration        {n3x}/30 DAGs on airflow_3x pool, rest airflow_2x   ok={mig_ok}")
    assert mig_ok

    # UC2 — Kubernetes worker migration (queue)
    nks_ok = all(
        (placements[d][1] == ("kubernetes" if d in K8S_WORKER_SET else "default")) for d in POPULATION
    )
    nk8s = sum(1 for d in POPULATION if placements[d][1] == "kubernetes")
    print(f"UC2  K8s worker migration  {nk8s}/30 DAGs on the kubernetes queue                ok={nks_ok}")
    assert nks_ok

    # UC3 — KubernetesExecutor concurrent pods (code-path gate, by cluster)
    enabled_clusters = [c for c in CLUSTERS if flag_enabled("airflow.executor.k8s_concurrent_pod_creation", c, cluster_id=c)]
    uc3_ok = set(enabled_clusters) == CLUSTER_SET
    print(f"UC3  k8s concurrent pods   {len(enabled_clusters)}/20 clusters enabled (code-path gate)      ok={uc3_ok}")
    assert uc3_ok

    # UC4 — Disruption checkpointing (code-path gate, by task cohort)
    ckpt_on = [d for d in POPULATION if flag_enabled("airflow.task.resumable_checkpointing", f"{d}:only", dag_id=d)]
    uc4_ok = set(ckpt_on) == CKPT_SET
    print(f"UC4  disruption checkpoint {len(ckpt_on)}/30 tasks get resumable checkpointing (gate)   ok={uc4_ok}")
    assert uc4_ok

    # Backend portability: UC1 (migration routing) is identical on GrowthBook / in-house / Statsig
    print("\n" + "-" * 78)
    print("Backend portability of UC1 (2->3 migration routing) — same policy, other backends:")
    from openfeature_airflow.providers.growthbook import GrowthBookProvider
    from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider

    def _mig_pools():
        return {d: p for d, (p, _) in _parse(_write_population()).items()}

    flagd_pools = _mig_pools()
    api.set_provider(
        GrowthBookProvider(
            features={
                "airflow.task.pool": {
                    "defaultValue": "airflow_2x",
                    "rules": [{"condition": {"dag_id": {"$in": list(MIG_SET)}}, "force": "airflow_3x"}],
                }
            }
        )
    )
    gb_pools = _mig_pools()
    api.set_provider(
        InHouseTreatmentProvider(
            string_flags={
                "airflow.task.pool": {
                    "segments": [{"attribute": "dag_id", "in": list(MIG_SET), "variant": "airflow_3x"}],
                    "default": "airflow_2x",
                }
            }
        )
    )
    inhouse_pools = _mig_pools()
    identical = flagd_pools == gb_pools == inhouse_pools
    print(f"    flagd == GrowthBook == in-house for all 30 DAGs: {identical}")
    assert identical

    print("\n" + "=" * 78)
    print("ALL FOUR ORIGINAL USE CASES GATED LIVE; migration routing backend-portable — OK")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
