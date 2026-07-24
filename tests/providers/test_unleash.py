from __future__ import annotations

from openfeature.evaluation_context import EvaluationContext

from openfeature_airflow.providers.unleash import UnleashProvider


class FakeUnleash:
    """Duck-typed stand-in for UnleashClient (is_enabled / get_variant)."""

    def __init__(self, enabled_for=(), variant=None):
        self._enabled_for = set(enabled_for)
        self._variant = variant

    def is_enabled(self, feature, context, default=False):
        props = context.get("properties", {})
        return context.get("userId") in self._enabled_for or props.get("dag_id") in self._enabled_for

    def get_variant(self, feature, context):
        return self._variant or {"name": "disabled", "enabled": False}


def _ctx(entity, **attrs):
    return EvaluationContext(targeting_key=entity, attributes=attrs)


class TestUnleashProvider:
    def test_boolean_toggle(self):
        p = UnleashProvider(FakeUnleash(enabled_for=["e1"]))
        assert p.resolve_boolean_details("f", False, _ctx("e1")).value is True
        assert p.resolve_boolean_details("f", False, _ctx("e2")).value is False

    def test_string_from_variant_payload(self):
        p = UnleashProvider(FakeUnleash(variant={"name": "v", "enabled": True, "payload": {"value": "canary_pool"}}))
        assert p.resolve_string_details("f", "d", _ctx("e1")).value == "canary_pool"

    def test_enabled_values_fallback_for_plain_toggle(self):
        p = UnleashProvider(FakeUnleash(enabled_for=["e1"]), enabled_values={"f": "canary_pool"})
        assert p.resolve_string_details("f", "d", _ctx("e1")).value == "canary_pool"
        assert p.resolve_string_details("f", "d", _ctx("e2")).value == "d"
