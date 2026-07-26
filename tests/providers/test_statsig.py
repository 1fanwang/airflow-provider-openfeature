from __future__ import annotations

import pytest

pytest.importorskip("statsig")

from openfeature.evaluation_context import EvaluationContext  # noqa: E402

from openfeature_airflow.providers.statsig import StatsigProvider  # noqa: E402


class FakeStatsig:
    """Stand-in for the statsig module: check_gate(StatsigUser, gate)."""

    def __init__(self, pass_users=()):
        self._pass = set(pass_users)

    def check_gate(self, user, gate):
        return user.user_id in self._pass


def _ctx(entity, **attrs):
    return EvaluationContext(targeting_key=entity, attributes=attrs)


class TestStatsigProvider:
    def test_boolean_gate(self):
        p = StatsigProvider(FakeStatsig(pass_users=["e1"]), gate_map={"f": "g"})
        assert p.resolve_boolean_details("f", False, _ctx("e1")).value is True
        assert p.resolve_boolean_details("f", False, _ctx("e2")).value is False

    def test_string_via_enabled_values(self):
        p = StatsigProvider(FakeStatsig(pass_users=["e1"]), gate_map={"f": "g"}, enabled_values={"f": "canary_pool"})
        assert p.resolve_string_details("f", "d", _ctx("e1")).value == "canary_pool"
        assert p.resolve_string_details("f", "d", _ctx("e2")).value == "d"

    def test_string_defaults_when_gate_has_no_enabled_value(self):
        p = StatsigProvider(FakeStatsig(pass_users=["e1"]), gate_map={"f": "g"})
        assert p.resolve_string_details("f", "default", _ctx("e1")).value == "default"

    def test_default_resolvers(self):
        p = StatsigProvider(FakeStatsig())
        ctx = _ctx("e1")
        assert p.resolve_integer_details("i", 1, ctx).value == 1
        assert p.resolve_float_details("f", 1.5, ctx).value == 1.5
        assert p.resolve_object_details("o", {"a": 1}, ctx).value == {"a": 1}

    def test_track_errors_are_swallowed(self):
        class BadStatsig(FakeStatsig):
            def log_event(self, event):
                raise RuntimeError("statsig down")

        StatsigProvider(BadStatsig()).track("metric", _ctx("e1"))
