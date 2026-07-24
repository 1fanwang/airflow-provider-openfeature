"""airflow_local_settings for the multi-backend e2e, wires the OpenFeature placement policy.

Airflow imports this at settings.initialize(); it delegates to the package's ``apply_placement`` so
DagBag parsing runs the real flag-driven policy against whichever provider the driver registered.
"""

from __future__ import annotations

from openfeature_airflow.policy import apply_placement


def task_policy(task):
    apply_placement(task)

