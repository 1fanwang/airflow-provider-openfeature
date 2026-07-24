"""Template for wrapping a proprietary/in-house experiment engine behind OpenFeature.

Most in-house engines expose ``getTreatment(key, entity, context)`` with deterministic bucketing and
attribute targeting. This provider shows the shape: attribute-segment targeting first, then a
deterministic percentage rollout, so an in-house backend gates identically to flagd / GrowthBook /
Unleash through the same policy. Swap the internals for your engine's client.
"""

from __future__ import annotations

import hashlib

from openfeature.flag_evaluation import FlagResolutionDetails, Reason
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

from openfeature_airflow.providers.fractional import entity_of


def _bucket(entity: str, flag_key: str) -> int:
    digest = hashlib.sha256(f"{flag_key}:{entity}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % 100


class InHouseTreatmentProvider(AbstractProvider):
    """Segment-targeting + percentage-rollout provider driven by a config dict.

    ``string_flags`` maps a flag key to::

        {
          "segments": [{"attribute": "dag_id", "in": [...], "variant": "canary_pool"}],
          "rollout": [("canary_pool", 20), ("default_pool", 80)],   # optional, deterministic
          "default": "default_pool",
        }
    """

    def __init__(self, string_flags: dict | None = None) -> None:
        self._flags = string_flags or {}

    def get_metadata(self) -> Metadata:
        return Metadata(name="InHouseTreatmentProvider")

    def get_provider_hooks(self):
        return []

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        cfg = self._flags.get(flag_key)
        if cfg is None:
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)
        attrs = (evaluation_context.attributes or {}) if evaluation_context is not None else {}
        for seg in cfg.get("segments", []):
            if attrs.get(seg["attribute"]) in seg["in"]:
                variant = seg["variant"]
                return FlagResolutionDetails(value=variant, variant=variant, reason=Reason.TARGETING_MATCH)
        rollout = cfg.get("rollout")
        if rollout:
            bucket = _bucket(entity_of(evaluation_context), flag_key)
            cumulative = 0
            for value, weight in rollout:
                cumulative += weight
                if bucket < cumulative:
                    return FlagResolutionDetails(value=value, variant=value, reason=Reason.TARGETING_MATCH)
        if "default" in cfg:
            return FlagResolutionDetails(value=cfg["default"], variant=cfg["default"], reason=Reason.TARGETING_MATCH)
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        details = self.resolve_string_details(flag_key, "", evaluation_context)
        if details.reason == Reason.DEFAULT:
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)
        return FlagResolutionDetails(value=details.value not in ("", "off", "false"), reason=Reason.TARGETING_MATCH)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

