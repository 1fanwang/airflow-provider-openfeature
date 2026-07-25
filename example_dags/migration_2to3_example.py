"""Airflow 2 to 3 migration: route a cohort of DAGs to a 3.x worker pool, ramp, and revert.

Migrating every DAG to Airflow 3 at once is unsafe: scheduling-semantic changes, removed in-task DB
access, and REST API changes mean each DAG needs to run against the new runtime before you trust it.
Airflow's own guidance is to stand up a separate 3.x worker pool and move DAGs onto it a cohort at a
time. This is that, driven by a flag instead of by editing each DAG.

Setup (once): enable the policy and register a backend (see docs/getting-started.md), then define
`airflow.task.pool` in your backend to return `airflow_3x` for the migrating cohort and `airflow_2x`
for the rest. Widen the cohort to ramp; empty it to roll back. No DAG edits, no redeploy.

The DAG below asks for `airflow_2x`; the policy overrides it to `airflow_3x` while this DAG is in the
migration cohort.
"""

from __future__ import annotations

import datetime

from airflow import DAG

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="migration_2to3_example",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["openfeature", "example", "migration"],
) as dag:
    EmptyOperator(task_id="extract", pool="airflow_2x")
    EmptyOperator(task_id="load", pool="airflow_2x")
