"""OpenFeature provider backed by an Unleash client. Install: ``pip install UnleashClient``.

Duck-typed over the client so this imports without the SDK: inject a live ``UnleashClient`` that
exposes ``is_enabled(feature, context, default)`` and ``get_variant(feature, context)``.
"""

from __future__ import annotations

from openfeature.flag_evaluation import FlagResolutionDetails, Reason
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

from openfeature_airflow.providers.fractional import entity_of


class UnleashProvider(AbstractProvider):
    def __init__(self, unleash_client, context_field: str = "userId", enabled_values: dict | None = None) -> None:
        self._client = unleash_client
        self._field = context_field
        # Optional: map a boolean toggle (enabled via a constraint) to a string value, for Unleash
        # setups that gate a cohort without variants.
        self._enabled_values = enabled_values or {}

    def get_metadata(self) -> Metadata:
        return Metadata(name="UnleashProvider")

    def get_provider_hooks(self):
        return []

    def _context(self, ctx) -> dict:
        out = {self._field: entity_of(ctx)}
        if ctx is not None and ctx.attributes:
            out["properties"] = {k: str(v) for k, v in ctx.attributes.items() if v is not None}
        return out

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        on = self._client.is_enabled(flag_key, self._context(evaluation_context), default_value)
        return FlagResolutionDetails(value=bool(on), reason=Reason.TARGETING_MATCH)

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        context = self._context(evaluation_context)
        v = self._client.get_variant(flag_key, context)
        name = v.get("name") if isinstance(v, dict) else getattr(v, "name", None)
        enabled = v.get("enabled") if isinstance(v, dict) else getattr(v, "enabled", False)
        payload = v.get("payload") if isinstance(v, dict) else getattr(v, "payload", None)
        if enabled and name and name != "disabled":
            value = payload.get("value") if isinstance(payload, dict) else name
            return FlagResolutionDetails(value=str(value), variant=str(name), reason=Reason.TARGETING_MATCH)
        # No usable variant: fall back to the boolean toggle mapped to a value.
        mapped = self._enabled_values.get(flag_key)
        if mapped is not None and self._client.is_enabled(flag_key, context, False):
            return FlagResolutionDetails(value=str(mapped), variant=str(mapped), reason=Reason.TARGETING_MATCH)
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

