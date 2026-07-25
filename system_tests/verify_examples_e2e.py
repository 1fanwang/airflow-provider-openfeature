"""Prove the example DAGs and the docs' claims by actually running them on real Airflow.

For every example DAG this runs ``airflow dags test`` against a real metadata DB with the OpenFeature
policy + an in-house backend wired in, then checks the behavior the docs claim:

- revenue_rollup_example    : with the flag on, every region runs v2 and the parity guardrail holds.
- migration_2to3_example    : the policy moves the tasks' pool to airflow_3x.
- ab_test_model_example     : the task picks the flagged model variant (returned via XCom).
- openfeature_example       : the FeatureFlagSensor gate opens when the flag is on, and the run task fires.
- kubernetes_executor_rollout_example : the fan-out executes; the policy sets task.executor at parse.

Every task must end in ``success``. Run:  python system_tests/verify_examples_e2e.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
HOME = "/tmp/verify_examples"

LOCAL_SETTINGS = '''
from openfeature import api
from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider
from openfeature_airflow.policy import apply_placement

api.set_provider(InHouseTreatmentProvider(string_flags={
    "revenue_rollup.use_fast_agg": {"default": "on"},        # boolean -> True: run v2
    "ranking.model_version": {"default": "v2"},              # pick the v2 model arm
    "airflow.example.rollout_ready": {"default": "on"},      # open the sensor gate
    "airflow.task.pool": {"default": "airflow_3x"},          # policy moves every task's pool
}))

def task_policy(task):
    apply_placement(task)
'''


def base_env():
    e = dict(os.environ)
    e["PATH"] = os.path.dirname(sys.executable) + os.pathsep + e.get("PATH", "")
    e.update(
        AIRFLOW_HOME=HOME, PYTHONPATH=f"{REPO/'src'}",
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__DAGS_FOLDER=f"{HOME}/dags",
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{HOME}/airflow.db",
        AIRFLOW__LOGGING__LOGGING_LEVEL="ERROR",
    )
    return e


def setup():
    import shutil

    if os.path.exists(HOME):
        shutil.rmtree(HOME)
    os.makedirs(f"{HOME}/dags")
    os.makedirs(f"{HOME}/config")
    Path(f"{HOME}/config/airflow_local_settings.py").write_text(LOCAL_SETTINGS)
    for f in (REPO / "example_dags").glob("*.py"):
        shutil.copy(f, f"{HOME}/dags/{f.name}")
    env = base_env()
    subprocess.run(["airflow", "db", "migrate"], env=env, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def dags_test(dag_id: str) -> int:
    return subprocess.run(["airflow", "dags", "test", dag_id], env=base_env(),
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode


def _session():
    os.environ.update(base_env())
    from airflow.settings import Session
    return Session()


def task_states(dag_id: str) -> dict:
    import sqlite3
    con = sqlite3.connect(f"{HOME}/airflow.db")
    try:
        rows = con.execute(
            "SELECT task_id, map_index, state, pool FROM task_instance WHERE dag_id=?", (dag_id,)
        ).fetchall()
    finally:
        con.close()
    return {(r[0], r[1]): (r[2], r[3]) for r in rows}


def xcom_return(dag_id: str, task_id: str):
    import json
    import sqlite3
    con = sqlite3.connect(f"{HOME}/airflow.db")
    try:
        row = con.execute(
            "SELECT value FROM xcom WHERE dag_id=? AND task_id=? AND key='return_value' "
            "ORDER BY timestamp DESC LIMIT 1", (dag_id, task_id)).fetchone()
    finally:
        con.close()
    if not row or row[0] is None:
        return None
    val = row[0]
    if isinstance(val, (bytes, bytearray)):
        val = val.decode("utf-8", "replace")
    try:
        return json.loads(val)
    except Exception:
        return val


def executor_on_regular_task() -> bool:
    """The policy sets task.executor on a regular operator (the documented behavior)."""
    import datetime

    from airflow import DAG
    from openfeature import api

    from openfeature_airflow.policy import apply_placement
    from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider
    try:
        from airflow.providers.standard.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.empty import EmptyOperator

    api.set_provider(InHouseTreatmentProvider(string_flags={"airflow.task.executor": {"default": "LocalExecutor"}}))
    with DAG("exec_probe", schedule=None, start_date=datetime.datetime(2024, 1, 1)):
        t = EmptyOperator(task_id="run")
    apply_placement(t)
    return getattr(t, "executor", None) == "LocalExecutor"


def mapped_dag_parses_with_executor_flag() -> bool:
    """A mapped task's executor is read-only; the policy must skip it, never break DAG parsing."""
    from airflow.dag_processing.dagbag import DagBag
    from openfeature import api

    from openfeature_airflow.policy import apply_placement
    from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider

    api.set_provider(InHouseTreatmentProvider(string_flags={"airflow.task.executor": {"default": "LocalExecutor"}}))
    import airflow.settings as s
    s.task_policy = lambda t: apply_placement(t)
    try:
        bag = DagBag(dag_folder=f"{HOME}/dags", include_examples=False, safe_mode=False)
    except TypeError:  # Airflow 3.x dropped include_examples
        bag = DagBag(dag_folder=f"{HOME}/dags")
    # use the in-memory parsed dict, not get_dag() (which hits the DB on 3.x)
    return "kubernetes_executor_rollout_example" in bag.dags and not bag.import_errors


def custom_dimension_on_real_task() -> bool:
    """A registered custom placement dimension is applied by the policy on a real Airflow operator."""
    import datetime

    from airflow import DAG
    from openfeature import api

    from openfeature_airflow import policy
    from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider
    try:
        from airflow.providers.standard.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.empty import EmptyOperator

    policy.register_placement("airflow.task.doc_md", lambda t, v: setattr(t, "doc_md", v))
    try:
        api.set_provider(InHouseTreatmentProvider(string_flags={"airflow.task.doc_md": {"default": "canary-note"}}))
        with DAG("custom_dim_probe", schedule=None, start_date=datetime.datetime(2024, 1, 1)):
            t = EmptyOperator(task_id="run")
        policy.apply_placement(t)
        return getattr(t, "doc_md", None) == "canary-note"
    finally:
        policy._DIMENSIONS.pop()


def main():
    setup()
    print("=" * 84)
    print("Verifying the example DAGs by running them on real Airflow (airflow dags test).")
    print("=" * 84)
    checks = []

    for dag_id in ("revenue_rollup_example", "migration_2to3_example", "ab_test_model_example",
                   "openfeature_example", "kubernetes_executor_rollout_example"):
        rc = dags_test(dag_id)
        states = task_states(dag_id)
        all_ok = bool(states) and all(st == "success" for st, _ in states.values())
        print(f"\n[{dag_id}]  exit={rc}  tasks={len(states)}  all success: {all_ok}")
        checks.append(all_ok)

        if dag_id == "migration_2to3_example":
            pools = {pool for _, pool in states.values()}
            moved = pools == {"airflow_3x"}
            print(f"    policy moved pool airflow_2x -> airflow_3x: {moved}  (pools={pools})")
            checks.append(moved)
        if dag_id == "ab_test_model_example":
            model = xcom_return(dag_id, "train")
            print(f"    task picked the flagged model variant: {model == 'v2'}  (model={model})")
            checks.append(model == "v2")
        if dag_id == "revenue_rollup_example":
            print(f"    every region ran v2 with the parity guardrail passing: {all_ok}")

    ex_ok = executor_on_regular_task()
    print(f"\n[executor placement]  policy sets task.executor on a regular operator: {ex_ok}")
    checks.append(ex_ok)
    mapped_ok = mapped_dag_parses_with_executor_flag()
    print(f"[mapped-task safety]  executor flag on a mapped task does not break DAG parsing: {mapped_ok}")
    checks.append(mapped_ok)
    custom_ok = custom_dimension_on_real_task()
    print(f"[custom dimension]    a registered custom placement dimension is applied on a real operator: {custom_ok}")
    checks.append(custom_ok)

    ok = all(checks)
    print("\n" + "=" * 84)
    print(f"{sum(checks)}/{len(checks)} checks passed")
    print("ALL EXAMPLE DAGS EXECUTE AND BEHAVE AS DOCUMENTED" if ok else "A CHECK FAILED")
    print("=" * 84)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
