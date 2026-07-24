"""Flag-driven placement cluster policy (progressive delivery).

Registered on the ``airflow.policy`` entry point so it loads automatically, but it is a no-op unless
``[openfeature] enable_policy = True`` is set -- installing the package changes nothing by default.

When enabled, it consults the globally-registered OpenFeature provider and, for each task, overrides
``pool`` / ``queue`` / ``executor`` when the corresponding flag resolves to a value for that task's
cohort. The backend (flagd, GrowthBook, Unleash, an in-house engine, ...) decides who is in which
cohort -- canary %, targeting rule, blue-green -- so a rollout is a backend config change, not a code
change. Keyed on ``dag_id:task_id``.
"""

from __future__ import annotations

from airflow.policies import hookimpl

# Well-known flags the default policy consults. A backend maps these to its own flags/experiments.
FLAG_POOL = "airflow.task.pool"
FLAG_QUEUE = "airflow.task.queue"
FLAG_EXECUTOR = "airflow.task.executor"
FLAG_PRIORITY_WEIGHT = "airflow.task.priority_weight"
_UNSET = "__unset__"
_UNSET_INT = -(2**31)


def _entity(task) -> str:
    dag_id = getattr(task, "dag_id", None) or getattr(getattr(task, "dag", None), "dag_id", "") or ""
    return f"{dag_id}:{getattr(task, 'task_id', '')}"


def apply_placement(task) -> None:
    """Override pool/queue/executor/priority for this task's cohort from the registered OpenFeature provider."""
    from openfeature_airflow.gate import number, variant

    entity = _entity(task)
    attrs = {"dag_id": getattr(task, "dag_id", ""), "task_id": getattr(task, "task_id", "")}

    pool = variant(FLAG_POOL, entity, _UNSET, **attrs)
    if pool != _UNSET:
        task.pool = pool
    queue = variant(FLAG_QUEUE, entity, _UNSET, **attrs)
    if queue != _UNSET:
        task.queue = queue
    executor = variant(FLAG_EXECUTOR, entity, _UNSET, **attrs)
    if executor != _UNSET and hasattr(task, "executor"):
        task.executor = executor
    priority = number(FLAG_PRIORITY_WEIGHT, entity, _UNSET_INT, **attrs)
    if priority != _UNSET_INT and hasattr(task, "priority_weight"):
        task.priority_weight = priority


def _policy_enabled() -> bool:
    from airflow.configuration import conf

    return conf.getboolean("openfeature", "enable_policy", fallback=False)


@hookimpl
def task_policy(task) -> None:
    if _policy_enabled():
        apply_placement(task)


def make_task_policy(*, enabled: bool = True):
    """Factory for wiring the policy explicitly in ``airflow_local_settings`` (ignores the config gate)."""

    def _task_policy(task) -> None:
        if enabled:
            apply_placement(task)

    return _task_policy

