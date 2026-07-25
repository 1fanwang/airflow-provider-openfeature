from __future__ import annotations

from pathlib import Path

from airflow.models.dagbag import DagBag

EXAMPLE_DIR = Path(__file__).resolve().parents[1] / "example_dags"
EXPECTED = {
    "openfeature_example",
    "migration_2to3_example",
    "kubernetes_executor_rollout_example",
    "ab_test_model_example",
    "revenue_rollup_example",
}


def test_example_dags_import_clean():
    try:
        bag = DagBag(dag_folder=str(EXAMPLE_DIR), include_examples=False)
    except TypeError:  # Airflow 3.x dropped the include_examples kwarg
        bag = DagBag(dag_folder=str(EXAMPLE_DIR))
    assert not bag.import_errors, bag.import_errors
    assert EXPECTED <= set(bag.dags)
