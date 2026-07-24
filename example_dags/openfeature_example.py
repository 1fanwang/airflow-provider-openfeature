"""Example: gate a task on a feature flag, and the cluster-policy wiring for progressive delivery."""

from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

from openfeature_airflow.sensors.feature_flag import FeatureFlagSensor

with DAG(
    dag_id="openfeature_example",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["openfeature", "example"],
) as dag:
    # Hold the pipeline until the rollout flag is enabled for this DAG's cohort.
    wait_for_rollout = FeatureFlagSensor(
        task_id="wait_for_rollout",
        flag_key="airflow.example.rollout_ready",
        mode="reschedule",
        poke_interval=30,
    )
    run = EmptyOperator(task_id="run")
    wait_for_rollout >> run

# Progressive delivery of the platform is separate: enable the policy and let the backend ramp
# `airflow.task.pool`. Wire it in airflow_local_settings.py:
#
#   from openfeature import api
#   from openfeature.contrib.provider.flagd import FlagdProvider
#   from openfeature_airflow.policy import apply_placement
#
#   api.set_provider(FlagdProvider(host="localhost", port=8013))
#
#   def task_policy(task):
#       apply_placement(task)   # or set [openfeature] enable_policy=True to auto-register

