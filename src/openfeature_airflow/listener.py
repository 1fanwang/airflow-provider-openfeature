"""Cohort/variant exposure: record which cohort each task landed in, for measurement.

``emit_exposure`` logs a structured exposure line and increments an ``openfeature.exposure`` metric,
so a downstream experiment platform can join the cohort assignment to task metrics. It is also wired
as an ``on_task_instance_running`` listener; that hook fires worker-side on Airflow 2.x. On Airflow 3.x
task-instance listeners live in the Task SDK, so prefer calling ``emit_exposure`` from your task or
from the policy. Everything here is a no-op unless ``[openfeature] enable_exposure_listener = True``.
"""

from __future__ import annotations

import logging

from airflow.listeners import hookimpl

log = logging.getLogger(__name__)

EXPOSURE_FLAGS = ("airflow.task.pool", "airflow.task.queue", "airflow.task.executor")
_UNSET = "__unset__"


def _enabled() -> bool:
    from airflow.configuration import conf

    return conf.getboolean("openfeature", "enable_exposure_listener", fallback=False)


def emit_exposure(dag_id: str, task_id: str, run_id: str | None = None, flags=EXPOSURE_FLAGS) -> dict:
    """Resolve the given flags for this task cohort and emit an exposure log + metric. Returns the map."""
    from openfeature_airflow.gate import variant

    entity = f"{dag_id}:{task_id}"
    treatments = {k: variant(k, entity, _UNSET, dag_id=dag_id, task_id=task_id) for k in flags}
    treatments = {k: v for k, v in treatments.items() if v != _UNSET}
    if treatments:
        log.info(
            "openfeature.exposure dag_id=%s task_id=%s run_id=%s treatments=%s",
            dag_id,
            task_id,
            run_id,
            treatments,
        )
        try:
            try:
                from airflow.sdk.observability.stats import Stats  # Airflow 3.x
            except ImportError:
                from airflow.stats import Stats  # Airflow 2.x

            for flag, value in treatments.items():
                Stats.incr("openfeature.exposure", tags={"flag": flag, "variant": value, "dag_id": dag_id})
        except Exception:  # metrics are best-effort
            pass
    return treatments


@hookimpl
def on_task_instance_running(previous_state=None, task_instance=None, session=None):
    if not _enabled() or task_instance is None:
        return
    try:
        emit_exposure(
            task_instance.dag_id, task_instance.task_id, getattr(task_instance, "run_id", None)
        )
    except Exception as exc:  # never break a task on exposure emission
        log.debug("openfeature exposure listener skipped: %s", exc)

