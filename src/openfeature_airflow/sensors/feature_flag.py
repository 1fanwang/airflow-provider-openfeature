from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

try:  # Airflow 3.x Task SDK compat shim
    from airflow.providers.common.compat.sdk import BaseSensorOperator
except ImportError:  # Airflow 2.x
    from airflow.sensors.base import BaseSensorOperator

from openfeature_airflow.hooks.openfeature import OpenFeatureHook

if TYPE_CHECKING:
    from airflow.sdk import Context


class FeatureFlagSensor(BaseSensorOperator):
    """
    Wait until a boolean feature flag resolves to the expected value for a targeting entity.

    Use it to gate a downstream task on a rollout reaching a cohort, for example holding a task until
    a flag is enabled for its ``dag_id``.

    :param flag_key: the OpenFeature flag key to evaluate.
    :param entity: the targeting key the decision is bucketed on (defaults to the task's ``dag_id``).
    :param conn_id: the :class:`~openfeature_airflow.hooks.openfeature.OpenFeatureHook` connection id.
    :param attributes: extra evaluation-context attributes a provider can target on.
    :param expected: the boolean value to wait for (``True`` by default).
    """

    template_fields: Sequence[str] = ("flag_key", "entity")

    def __init__(
        self,
        *,
        flag_key: str,
        entity: str | None = None,
        conn_id: str = OpenFeatureHook.default_conn_name,
        attributes: dict[str, Any] | None = None,
        expected: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.flag_key = flag_key
        self.entity = entity
        self.conn_id = conn_id
        self.attributes = attributes or {}
        self.expected = expected

    @cached_property
    def hook(self) -> OpenFeatureHook:
        return OpenFeatureHook(self.conn_id)

    def poke(self, context: Context) -> bool:
        entity = self.entity or context["dag"].dag_id
        return self.hook.is_enabled(self.flag_key, entity, default=False, **self.attributes) == self.expected

