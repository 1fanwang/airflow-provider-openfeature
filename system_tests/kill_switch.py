"""Kill switch / instant rollback: flip a flag and a subset reverts, with no code change or redeploy.

Places `dag_003` in a canary pool via the policy, then edits the flag config the flagd daemon watches
to drop the subset. flagd hot-reloads, a second real `airflow dags test` runs, and the task reverts to
the default pool. The only thing that changed between the two runs is the flag.

Prereqs: Docker (flagd). Run:  python system_tests/kill_switch.py
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
FLAGD_PORT = 8353
AIRFLOW_HOME = "/tmp/ofks"
FLAGS_DIR = "/tmp/ofks_flags"
DAG_ID = "dag_003"

ON = {"flags": {"airflow.task.pool": {
    "state": "ENABLED", "variants": {"canary": "canary_pool", "default": "default_pool"},
    "defaultVariant": "default",
    "targeting": {"if": [{"in": [{"var": "dag_id"}, [DAG_ID]]}, "canary", "default"]}}}}
# Kill switch: same flag, subset emptied -> everyone falls back to the default pool.
OFF = {"flags": {"airflow.task.pool": {
    "state": "ENABLED", "variants": {"canary": "canary_pool", "default": "default_pool"},
    "defaultVariant": "default", "targeting": {"if": [{"in": [{"var": "dag_id"}, []]}, "canary", "default"]}}}}

LOCAL_SETTINGS = f'''
import time
from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider
from openfeature_airflow.policy import apply_placement
api.set_provider(FlagdProvider(host="localhost", port={FLAGD_PORT})); time.sleep(2)
def task_policy(task):
    apply_placement(task)
'''


def setup():
    import shutil
    for d in (AIRFLOW_HOME, FLAGS_DIR):
        if Path(d).exists():
            shutil.rmtree(d)
    (Path(AIRFLOW_HOME) / "dags").mkdir(parents=True)
    (Path(AIRFLOW_HOME) / "config").mkdir()
    Path(FLAGS_DIR).mkdir(parents=True)
    (Path(AIRFLOW_HOME) / "config" / "airflow_local_settings.py").write_text(LOCAL_SETTINGS)
    (Path(AIRFLOW_HOME) / "dags" / f"{DAG_ID}.py").write_text(
        "from airflow import DAG\n"
        "from airflow.providers.standard.operators.empty import EmptyOperator\n"
        "import datetime\n"
        f"with DAG(dag_id='{DAG_ID}', schedule=None, start_date=datetime.datetime(2024,1,1), catchup=False) as dag:\n"
        "    EmptyOperator(task_id='only', pool='default_pool')\n")
    write_flags(ON)


def write_flags(cfg):
    Path(f"{FLAGS_DIR}/flags.json").write_text(json.dumps(cfg))


def env():
    e = dict(os.environ)
    e.update(AIRFLOW_HOME=AIRFLOW_HOME, PYTHONPATH=str(REPO / "src"),
             AIRFLOW__CORE__LOAD_EXAMPLES="False", AIRFLOW__CORE__DAGS_FOLDER=f"{AIRFLOW_HOME}/dags",
             AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{AIRFLOW_HOME}/airflow.db",
             AIRFLOW__LOGGING__LOGGING_LEVEL="ERROR")
    return e


def run_and_read():
    subprocess.run(["airflow", "dags", "test", DAG_ID], env=env(), check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    from airflow.models.taskinstance import TaskInstance
    from airflow.settings import Session
    s = Session()
    ti = s.query(TaskInstance).filter(TaskInstance.dag_id == DAG_ID).order_by(TaskInstance.start_date.desc()).first()
    s.close()
    return ti.pool, ti.state


def flagd_up():
    subprocess.run(["docker", "rm", "-f", "flagd-ks"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "run", "-d", "--name", "flagd-ks", "-p", f"{FLAGD_PORT}:8013",
                    "-v", f"{FLAGS_DIR}:/flags", "ghcr.io/open-feature/flagd:latest",
                    "start", "--uri", "file:/flags/flags.json"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(4)


def main():
    os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME
    setup()
    subprocess.run(["airflow", "db", "migrate"], env=env(), check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["airflow", "pools", "set", "canary_pool", "8", "canary"], env=env(),
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        flagd_up()
    except subprocess.CalledProcessError:
        print("flagd failed to start (Docker?)"); sys.exit(1)

    print("=" * 60)
    print("Kill switch, flip one flag, the subset reverts, no redeploy")
    print("=" * 60)
    pool_before, state_before = run_and_read()
    print(f"  RUN 1 (flag targets {DAG_ID}):        pool={pool_before!r} state={state_before!r}")

    write_flags(OFF)   # the kill switch: empty the subset in the config flagd watches
    time.sleep(4)      # flagd hot-reloads the mounted file
    pool_after, state_after = run_and_read()
    print("  ...flipped the flag (subset emptied); flagd hot-reloaded...")
    print(f"  RUN 2 (flag flipped):                 pool={pool_after!r} state={state_after!r}")

    subprocess.run(["docker", "rm", "-f", "flagd-ks"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ok = pool_before == "canary_pool" and pool_after == "default_pool" and state_after == "success"
    print("-" * 60)
    print("a single flag change reverted the placement; no DAG edit, no deploy"
          if ok else "FAILURE: the flag flip did not revert placement")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
