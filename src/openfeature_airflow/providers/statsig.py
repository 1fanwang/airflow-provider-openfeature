"""OpenFeature provider backed by the Statsig server SDK. Install: ``pip install statsig``.

Statsig is a gates + experiments platform. This adapter maps a boolean gate to a resolution, and
(optionally) a gate to a string value for placement flags. Pass the ``statsig`` module (already
``initialize``-d) or any object exposing ``check_gate(StatsigUser, gate)``.
"""

from __future__ import annotations

from openfeature.flag_evaluation import FlagResolutionDetails, Reason
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

from openfeature_airflow.providers.fractional import entity_of


class StatsigProvider(AbstractProvider):
    def __init__(self, statsig_module, gate_map: dict | None = None, enabled_values: dict | None = None) -> None:
        self._sg = statsig_module
        self._gate_map = gate_map or {}  # OpenFeature flag_key -> Statsig gate name
        self._enabled_values = enabled_values or {}  # flag_key -> value when the gate passes

    def get_metadata(self) -> Metadata:
        return Metadata(name="StatsigProvider")

    def get_provider_hooks(self):
        return []

    def _user(self, ctx):
        from statsig import StatsigUser

        entity = entity_of(ctx)
        custom = {k: str(v) for k, v in ((ctx.attributes or {}) if ctx is not None else {}).items() if v is not None}
        return StatsigUser(user_id=entity, custom=custom)

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        gate = self._gate_map.get(flag_key, flag_key)
        return FlagResolutionDetails(
            value=bool(self._sg.check_gate(self._user(evaluation_context), gate)), reason=Reason.TARGETING_MATCH
        )

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        gate = self._gate_map.get(flag_key, flag_key)
        if self._sg.check_gate(self._user(evaluation_context), gate):
            value = self._enabled_values.get(flag_key)
            if value is not None:
                return FlagResolutionDetails(value=str(value), variant=str(value), reason=Reason.TARGETING_MATCH)
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def track(self, tracking_event_name, evaluation_context=None, tracking_event_details=None):
        """Forward an experiment outcome to Statsig as a logged event (shows up in Pulse/metrics)."""
        try:
            from statsig import StatsigEvent

            value = getattr(tracking_event_details, "value", None)
            meta = {k: str(v) for k, v in (getattr(tracking_event_details, "attributes", None) or {}).items()}
            self._sg.log_event(StatsigEvent(self._user(evaluation_context), tracking_event_name, value, meta))
        except Exception:  # never break a task on outcome emission
            pass
