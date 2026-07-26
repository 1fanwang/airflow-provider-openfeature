"""Execution-level test for the gate use cases + the exposure listener, on real flagd.

A real ``airflow dags test`` runs a PythonOperator whose task body, inside the running task, evaluates:

    UC3  airflow.executor.k8s_concurrent_pod_creation   (code-path gate, keyed by cluster)
    UC4  airflow.task.resumable_checkpointing            (code-path gate, keyed by task)

and calls the exposure listener's ``emit_exposure`` (the measurement half). Results are written from
inside the task and read back here. In-subset -> (UC3 on, UC4 on, exposure pool=canary_pool);
out-of-subset -> (off, off, default_pool). Proves the gate + listener surfaces carry real backend data
during real task execution, not just at parse.

Prereqs: Docker (flagd). Run:  python system_tests/gates_listener.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
FLAGD_PORT = 8323
AIRFLOW_HOME = "/tmp/ofgl"
OUT_DIR = "/tmp/ofgl_out"
SUBSET = {"dag_000", "dag_001"}
IN_DAG, OUT_DAG = "dag_000", "dag_004"

LOCAL_SETTINGS = f'''
import time
from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider
api.set_provider(FlagdProvider(host="localhost", port={FLAGD_PORT})); time.sleep(2)
'''

DAG_TEMPLATE = '''
import datetime, json, os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

SUBSET = {{"dag_000", "dag_001"}}

def evaluate(**context):
    from openfeature_airflow.gate import flag_enabled
    from openfeature_airflow.listener import emit_exposure
    dag_id = "{dag_id}"
    cluster = "cluster_00" if dag_id in SUBSET else "cluster_99"
    uc3 = flag_enabled("airflow.executor.k8s_concurrent_pod_creation", cluster, cluster_id=cluster)
    uc4 = flag_enabled("airflow.task.resumable_checkpointing", dag_id + ":eval", dag_id=dag_id)
    exposure = emit_exposure(dag_id, "eval")  # the listener's core, run inside the task
    os.makedirs("{out_dir}", exist_ok=True)
    with open("{out_dir}/" + dag_id + ".json", "w") as fh:
        json.dump({{"uc3": uc3, "uc4": uc4, "exposure": exposure}}, fh)

with DAG(dag_id="{dag_id}", schedule=None, start_date=datetime.datetime(2024,1,1), catchup=False) as dag:
    PythonOperator(task_id="eval", python_callable=evaluate)
'''


def setup():
    import shutil
    for d in (AIRFLOW_HOME, OUT_DIR):
        if Path(d).exists():
            shutil.rmtree(d)
    (Path(AIRFLOW_HOME) / "dags").mkdir(parents=True)
    (Path(AIRFLOW_HOME) / "config").mkdir()
    (Path(AIRFLOW_HOME) / "config" / "airflow_local_settings.py").write_text(LOCAL_SETTINGS)
    for dag_id in (IN_DAG, OUT_DAG):
        (Path(AIRFLOW_HOME) / "dags" / f"{dag_id}.py").write_text(
            DAG_TEMPLATE.format(dag_id=dag_id, out_dir=OUT_DIR))


def env():
    e = dict(os.environ)
    e.update(AIRFLOW_HOME=AIRFLOW_HOME, PYTHONPATH=str(REPO / "src"),
             AIRFLOW__CORE__LOAD_EXAMPLES="False", AIRFLOW__CORE__DAGS_FOLDER=f"{AIRFLOW_HOME}/dags",
             AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{AIRFLOW_HOME}/airflow.db",
             AIRFLOW__LOGGING__LOGGING_LEVEL="ERROR")
    return e


def flagd_up():
    subprocess.run(["docker", "rm", "-f", "flagd-gl"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "run", "-d", "--name", "flagd-gl", "-p", f"{FLAGD_PORT}:8013",
                    "-v", f"{HERE / 'flags'}:/flags", "ghcr.io/open-feature/flagd:latest",
                    "start", "--uri", "file:/flags/gates_flags.json"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)


def main():
    os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME
    setup()
    subprocess.run(["airflow", "db", "migrate"], env=env(), check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        flagd_up()
    except subprocess.CalledProcessError:
        print("flagd failed to start (Docker?)"); sys.exit(1)

    for dag_id in (IN_DAG, OUT_DAG):
        subprocess.run(["airflow", "dags", "test", dag_id], env=env(), check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print("=" * 74)
    print("GATE + LISTENER at execution (real flagd; evaluated inside the running task)")
    print("=" * 74)
    print(f"{'dag':<10}{'UC3 k8s pods':<14}{'UC4 checkpoint':<16}{'listener exposure (pool)'}")
    print("-" * 74)
    results = {}
    for dag_id in (IN_DAG, OUT_DAG):
        data = json.loads(Path(f"{OUT_DIR}/{dag_id}.json").read_text())
        results[dag_id] = data
        pool = data["exposure"].get("airflow.task.pool", "-")
        print(f"{dag_id:<10}{str(data['uc3']):<14}{str(data['uc4']):<16}{pool}")

    ok = (results[IN_DAG]["uc3"] is True and results[IN_DAG]["uc4"] is True
          and results[IN_DAG]["exposure"].get("airflow.task.pool") == "canary_pool"
          and results[OUT_DAG]["uc3"] is False and results[OUT_DAG]["uc4"] is False
          and results[OUT_DAG]["exposure"].get("airflow.task.pool") == "default_pool")

    subprocess.run(["docker", "rm", "-f", "flagd-gl"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("-" * 74)
    print("UC3, UC4 gates and the exposure listener all carried real flagd data through task execution"
          if ok else "FAILURE: gate/listener did not match the subset")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
