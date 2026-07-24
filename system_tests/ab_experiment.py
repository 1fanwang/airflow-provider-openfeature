"""A/B experiment inside a DAG: a task picks a model variant from a flag and records its arm.

Each DAG's `predict` task, running under a real `airflow dags test`, reads `ml.ranking_model` from real
flagd (a 50/50 fractional split) and runs the chosen model; the exposure listener records which arm the
run landed in, so the result is measurable downstream. Shows the split across a cohort and the exposure
each run emitted.

Prereqs: Docker (flagd). Run:  python system_tests/ab_experiment.py
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
FLAGD_PORT = 8343
AIRFLOW_HOME = "/tmp/ofab"
OUT_DIR = "/tmp/ofab_out"
POPULATION = [f"dag_{i:03d}" for i in range(8)]

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

def predict(**context):
    from openfeature_airflow.gate import variant
    from openfeature_airflow.listener import emit_exposure
    dag_id = "{dag_id}"
    entity = dag_id + ":predict"
    model = variant("ml.ranking_model", entity, "ranker_v1")   # real flagd A/B split
    score = {{"ranker_v1": 0.71, "ranker_v2": 0.79}}.get(model, 0.0)  # stand-in for a real model call
    exposure = emit_exposure(dag_id, "predict", flags=("ml.ranking_model",))  # record the arm
    os.makedirs("{out_dir}", exist_ok=True)
    with open("{out_dir}/" + dag_id + ".json", "w") as fh:
        json.dump({{"model": model, "score": score, "exposure": exposure}}, fh)

with DAG(dag_id="{dag_id}", schedule=None, start_date=datetime.datetime(2024,1,1), catchup=False) as dag:
    PythonOperator(task_id="predict", python_callable=predict)
'''


def setup():
    import shutil
    for d in (AIRFLOW_HOME, OUT_DIR):
        if Path(d).exists():
            shutil.rmtree(d)
    (Path(AIRFLOW_HOME) / "dags").mkdir(parents=True)
    (Path(AIRFLOW_HOME) / "config").mkdir()
    (Path(AIRFLOW_HOME) / "config" / "airflow_local_settings.py").write_text(LOCAL_SETTINGS)
    for dag_id in POPULATION:
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
    subprocess.run(["docker", "rm", "-f", "flagd-ab"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "run", "-d", "--name", "flagd-ab", "-p", f"{FLAGD_PORT}:8013",
                    "-v", f"{HERE / 'flags'}:/flags", "ghcr.io/open-feature/flagd:latest",
                    "start", "--uri", "file:/flags/ab_flags.json"], check=True,
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

    for dag_id in POPULATION:
        subprocess.run(["airflow", "dags", "test", dag_id], env=env(), check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print("=" * 66)
    print("A/B experiment in a DAG (real flagd 50/50 split; arm chosen inside the task)")
    print("=" * 66)
    print(f"{'dag':<10}{'model arm':<14}{'score':<8}{'exposure recorded'}")
    print("-" * 66)
    arms = {"ranker_v1": 0, "ranker_v2": 0}
    for dag_id in POPULATION:
        d = json.loads(Path(f"{OUT_DIR}/{dag_id}.json").read_text())
        arms[d["model"]] = arms.get(d["model"], 0) + 1
        print(f"{dag_id:<10}{d['model']:<14}{d['score']:<8}{d['exposure']}")

    subprocess.run(["docker", "rm", "-f", "flagd-ab"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("-" * 66)
    print(f"arm split: ranker_v1={arms['ranker_v1']}  ranker_v2={arms['ranker_v2']}  (of {len(POPULATION)})")
    ok = arms["ranker_v1"] > 0 and arms["ranker_v2"] > 0  # both arms exercised by the real split
    print("real flagd A/B data chose the model per run; exposure recorded for measurement"
          if ok else "FAILURE: the split did not exercise both arms")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
