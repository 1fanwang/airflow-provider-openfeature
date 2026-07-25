"""Flag-driven placement cluster policy (progressive delivery).

Registered on the ``airflow.policy`` entry point so it loads automatically, but it is a no-op unless
``[openfeature] enable_policy = True`` is set -- installing the package changes nothing by default.

When enabled, it consults the globally-registered OpenFeature provider and, for each task, applies every
registered placement dimension whose flag resolves to a value for that task's cohort. The four built-in
dimensions cover ``pool`` / ``queue`` / ``executor`` / ``priority_weight``; register more with
``register_placement`` to flag-drive any operator attribute. The backend (flagd, GrowthBook, Unleash, an
in-house engine, ...) decides who is in which cohort -- canary %, targeting rule, blue-green -- so a
rollout is a backend config change, not a code change. Keyed on ``dag_id:task_id``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from airflow.policies import hookimpl

log = logging.getLogger(__name__)

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


def _try_set(task, attr: str, value) -> None:
    """Set an attribute, skipping task types where it is read-only (e.g. a mapped task's executor)."""
    try:
        setattr(task, attr, value)
    except AttributeError:
        pass


@dataclass
class PlacementDimension:
    """A flag-driven placement: when ``flag_key`` resolves for a task's cohort, run ``setter(task, value)``.

    ``kind`` selects the resolver: ``"string"`` reads a variant, ``"number"`` reads an integer.
    """

    flag_key: str
    setter: Callable[[object, object], None]
    kind: str = "string"


_DIMENSIONS: list[PlacementDimension] = []


def register_placement(flag_key: str, setter: Callable[[object, object], None], *, kind: str = "string") -> None:
    """Add a custom flag-driven placement dimension the policy applies alongside the built-ins.

    ``setter(task, value)`` runs when ``flag_key`` resolves to a value for the task's cohort; use it to
    set any operator attribute (a canary executor, a Spark version, an ``enable_checkpoint`` boolean via
    ``lambda t, v: setattr(t, "enable_checkpoint", v == "true")``). ``kind`` is ``"string"`` (a variant)
    or ``"number"`` (an int). Call it from ``airflow_local_settings`` or a bootstrap. A setter that
    raises is skipped, so a policy never breaks DAG parsing.
    """
    _DIMENSIONS.append(PlacementDimension(flag_key, setter, kind))


def _attr(name: str) -> Callable[[object, object], None]:
    return lambda task, value: _try_set(task, name, value)


register_placement(FLAG_POOL, _attr("pool"))
register_placement(FLAG_QUEUE, _attr("queue"))
register_placement(FLAG_EXECUTOR, _attr("executor"))
register_placement(FLAG_PRIORITY_WEIGHT, _attr("priority_weight"), kind="number")


def apply_placement(task) -> None:
    """Apply every registered placement dimension whose flag resolves for this task's cohort."""
    from openfeature_airflow.gate import number, variant

    entity = _entity(task)
    attrs = {"dag_id": getattr(task, "dag_id", ""), "task_id": getattr(task, "task_id", "")}
    for dim in _DIMENSIONS:
        try:
            if dim.kind == "number":
                value = number(dim.flag_key, entity, _UNSET_INT, **attrs)
                if value == _UNSET_INT:
                    continue
            else:
                value = variant(dim.flag_key, entity, _UNSET, **attrs)
                if value == _UNSET:
                    continue
            dim.setter(task, value)
        except Exception as exc:  # a cluster policy must never break DAG parsing
            log.debug("openfeature placement %s skipped: %s", dim.flag_key, exc)


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

