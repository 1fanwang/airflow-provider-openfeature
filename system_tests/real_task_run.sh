#!/usr/bin/env bash
# Real eval data flowing into a real task execution: a live flagd change flips the pool a real
# Airflow TaskInstance runs in. Requires the use-case flagd container on :8113 (see E2E.md).
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
export AIRFLOW_HOME=/tmp/realrun
rm -rf /tmp/realrun && mkdir -p /tmp/realrun/dags /tmp/realrun/cfg
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/realrun/airflow.db
export AIRFLOW__CORE__DAGS_FOLDER=/tmp/realrun/dags
export AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR
export PYTHONPATH="/tmp/realrun/cfg:$REPO/src"

cat > /tmp/realrun/cfg/airflow_local_settings.py <<'PY'
import time
from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider
from openfeature_airflow.policy import apply_placement
api.set_provider(FlagdProvider(host="localhost", port=8113)); time.sleep(1)
def task_policy(task):
    apply_placement(task)
PY

cat > /tmp/realrun/dags/dag_003.py <<'PY'
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
import datetime
with DAG(dag_id="dag_003", schedule=None, start_date=datetime.datetime(2024,1,1), catchup=False) as dag:
    EmptyOperator(task_id="only", pool="airflow_2x")
PY

airflow db migrate >/dev/null 2>&1
airflow pools set airflow_3x 8 "3x pool" >/dev/null 2>&1
airflow pools set airflow_2x 8 "2x pool" >/dev/null 2>&1

show_pool() {
  python - <<'PY'
from airflow.settings import Session
from airflow.models.taskinstance import TaskInstance
ti = Session().query(TaskInstance).filter(TaskInstance.dag_id=="dag_003").order_by(TaskInstance.start_date.desc()).first()
print(f"  dag_003.only ran with pool={ti.pool!r} state={ti.state!r}")
PY
}

echo "RUN #1 (dag_003 in the migration cohort):"
airflow dags test dag_003 >/dev/null 2>&1; show_pool

echo "flip the flag (drop dag_003), let flagd hot-reload, run again — see the pool change:"
# edit $REPO/system_tests/flags/use_case_flags.json to exclude dag_003, sleep 3, re-run, show_pool, restore.
