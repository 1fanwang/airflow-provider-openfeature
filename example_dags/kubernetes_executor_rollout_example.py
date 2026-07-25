"""Canary a risky KubernetesExecutor change by routing a cohort of DAGs to a canary executor.

KubernetesExecutor creates worker pods one at a time inside the scheduler heartbeat, bounded by
`worker_pods_creation_batch_size` (default 1). On large fan-outs this serializes pod creation and
inflates `task.queued_duration`. apache/airflow#68480 ("Add opt-in concurrent pod creation to
KubernetesExecutor") fixes it, opt-in and off by default, with a benchmarked p99 improvement.

A change like that is exactly what you want to roll out to a few DAGs first. Enable it on a canary
KubernetesExecutor, then route a cohort there with `airflow.task.executor` while the rest stay on the
default. Watch `queued_duration`, ramp the cohort, and flip the flag off to revert the moment anything
looks off. Deployment-canary tools (Argo Rollouts, Flagger) can't do this: they shift HTTP traffic
between pod versions and explicitly do not support queue workers, so they have no way to say "send
these DAGs to the canary executor."

Per-task `executor` needs Airflow 3.x (or 2.10+ with multiple executors configured). Register the
executors in your Airflow config, then define `airflow.task.executor` in your backend to return the
canary executor's name for the cohort.
"""

from __future__ import annotations

import datetime

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator

with DAG(
    dag_id="kubernetes_executor_rollout_example",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["openfeature", "example", "kubernetes"],
) as dag:
    # Large dynamic fan-out: the kind of DAG whose queued_duration the #68480 change improves.
    PythonOperator.partial(task_id="process", python_callable=lambda shard: shard).expand(
        op_args=[[i] for i in range(20)]
    )

