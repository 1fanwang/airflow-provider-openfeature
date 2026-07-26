"""Hello-world runtime smoke tests: dead-simple checks that the core paths actually run.

Import and compile passing does not prove the code runs -- a tuple-keyed XCom return imported fine and
failed only at runtime on Airflow 2.11. These exercise real objects at runtime and should always be
green; if one breaks, something basic is wrong.
"""

from __future__ import annotations

import datetime


def test_package_and_submodules_import():
    import openfeature_airflow  # noqa: F401
    from openfeature_airflow import analysis, gate, listener, measure, policy, switchback  # noqa: F401
    from openfeature_airflow.providers import fractional, inhouse  # noqa: F401


def test_entry_point_targets_resolve():
    # the three entry points Airflow loads at runtime
    import openfeature_airflow.policy as policy  # airflow.policy
    from openfeature_airflow.plugin import OpenFeaturePlugin  # airflow.plugins
    from openfeature_airflow.provider_info import get_provider_info  # apache_airflow_provider

    assert callable(policy.apply_placement)
    assert get_provider_info()["package-name"] == "airflow-provider-openfeature"
    assert OpenFeaturePlugin.name


def test_policy_moves_a_real_airflow_task_at_runtime():
    from airflow import DAG

    try:
        from airflow.providers.standard.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.empty import EmptyOperator
    from openfeature import api

    from openfeature_airflow.policy import apply_placement
    from openfeature_airflow.providers.fractional import FractionalProvider, VariantFlag

    api.set_provider(FractionalProvider(
        variant_flags={"airflow.task.pool": VariantFlag([("canary_pool", 100), ("default_pool", 0)])}))
    with DAG("smoke", schedule=None, start_date=datetime.datetime(2024, 1, 1)):
        task = EmptyOperator(task_id="run", pool="default_pool")
    apply_placement(task)
    assert task.pool == "canary_pool"  # the real policy mutated a real task at runtime


def test_gate_returns_a_bool_through_the_client():
    from openfeature import api

    from openfeature_airflow.gate import flag_enabled
    from openfeature_airflow.providers.fractional import BoolFlag, FractionalProvider

    api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(100)}))
    assert flag_enabled("f", "e") is True
    assert flag_enabled("nope", "e") is False  # unknown flag -> caller default


def test_track_outcome_runs_without_error():
    from openfeature import api

    from openfeature_airflow.measure import track_outcome
    from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider

    api.set_provider(InHouseTreatmentProvider())
    track_outcome("task_duration_ms", "dag:task", value=123.4, variant="v2")


def test_quickstart_ramps_real_tasks_at_runtime():
    from openfeature_airflow._quickstart import build_tasks, place

    tasks = build_tasks()
    assert place(tasks, 0) == 0
    assert place(tasks, 100) == len(tasks)
