"""A/B a model version inside a task, and emit an exposure event for downstream analysis.

The modern experimentation architecture is: assign a variant, emit an exposure event, and measure
downstream in your warehouse. Eppo, Statsig Warehouse Native, and GrowthBook all work this way. Here
the randomization unit is the DAG run, the treatment is a model version, and the metric (model quality,
runtime, cost) lives in your warehouse. The exposure listener records which arm each run got, so you
can join it to those metrics later.

Define `ranking.model_version` in your backend as a weighted split (say 90/10 v1/v2). The task reads
it, runs that model, and records the arm. No policy needed for this one; it uses the in-task gate.
"""

from __future__ import annotations

import datetime

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator


def train_and_record(**context):
    from openfeature_airflow.gate import variant
    from openfeature_airflow.listener import emit_exposure

    dag_id = context["dag"].dag_id
    model = variant("ranking.model_version", f"{dag_id}:train", default="v1")
    # ... train / score with the chosen model here ...
    emit_exposure(dag_id, "train", flags=("ranking.model_version",))  # record the arm for analysis
    return model


with DAG(
    dag_id="ab_test_model_example",
    schedule=None,
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["openfeature", "example", "experiment"],
) as dag:
    PythonOperator(task_id="train", python_callable=train_and_record)
