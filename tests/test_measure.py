"""Unit tests for the measure half: track_outcome routing + the provider track() bridges."""

from __future__ import annotations

import pytest
from openfeature import api
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

from openfeature_airflow.measure import _emit_metric, track_outcome
from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider


class _CapturingProvider(AbstractProvider):
    def __init__(self):
        self.tracked = []

    def get_metadata(self):
        return Metadata(name="capture")

    def get_provider_hooks(self):
        return []

    def _det(self, default_value):
        from openfeature.flag_evaluation import FlagResolutionDetails

        return FlagResolutionDetails(value=default_value)

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        return self._det(default_value)

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        return self._det(default_value)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return self._det(default_value)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return self._det(default_value)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return self._det(default_value)

    def track(self, name, evaluation_context=None, tracking_event_details=None):
        self.tracked.append((name, getattr(tracking_event_details, "value", None),
                             getattr(tracking_event_details, "attributes", None)))


def test_track_outcome_routes_to_provider_track():
    p = _CapturingProvider()
    api.set_provider(p)
    track_outcome("task_duration_ms", "dag_a:only", value=12.5, variant="fastpath", dag_id="dag_a")
    assert len(p.tracked) == 1
    name, value, attrs = p.tracked[0]
    assert name == "task_duration_ms"
    assert value == 12.5
    assert attrs["variant"] == "fastpath"
    assert attrs["dag_id"] == "dag_a"


def test_track_outcome_drops_none_attrs():
    p = _CapturingProvider()
    api.set_provider(p)
    track_outcome("m", "e", value=1.0, variant="control", missing=None)
    _, _, attrs = p.tracked[0]
    assert "missing" not in attrs


def test_track_outcome_safe_when_provider_has_no_track():
    class _Plain(_CapturingProvider):
        track = None  # emulate a provider without tracking

    api.set_provider(_Plain())
    track_outcome("m", "e", value=1.0)  # must not raise


def test_inhouse_track_captures_outcome():
    p = InHouseTreatmentProvider(string_flags={"f": {"default": "off"}})
    api.set_provider(p)
    track_outcome("task_duration_ms", "dag_b:only", value=7.0, variant="control", dag_id="dag_b")
    assert p.tracked == [
        {"metric": "task_duration_ms", "entity": "dag_b:only", "value": 7.0,
         "attrs": {"variant": "control", "dag_id": "dag_b"}}
    ]


def test_growthbook_on_track_callback_invoked():
    pytest.importorskip("growthbook")
    from openfeature_airflow.providers.growthbook import GrowthBookProvider

    seen = []
    p = GrowthBookProvider(features={}, on_track=lambda *a: seen.append(a))
    api.set_provider(p)
    track_outcome("conv", "user_1", value=1.0, variant="B")
    assert seen and seen[0][0] == "conv" and seen[0][1] == "user_1"


def test_statsig_track_calls_log_event():
    pytest.importorskip("statsig")
    from statsig import StatsigOptions
    from statsig import statsig as sg

    from openfeature_airflow.providers.statsig import StatsigProvider

    sg.initialize("secret-local", StatsigOptions(local_mode=True))
    events = []
    orig = sg.log_event
    sg.log_event = lambda e: events.append(e)
    try:
        api.set_provider(StatsigProvider(sg))
        track_outcome("task_duration_ms", "dag_c:only", value=9.0, variant="fastpath")
        assert len(events) == 1
        assert events[0].event_name == "task_duration_ms"
        assert events[0].value == 9.0
        assert events[0].metadata["variant"] == "fastpath"
    finally:
        sg.log_event = orig


def test_emit_metric_sends_count_and_value(monkeypatch):
    calls = []

    class FakeStats:
        @staticmethod
        def incr(name, tags=None):
            calls.append(("incr", name, tags))

        @staticmethod
        def gauge(name, value, tags=None):
            calls.append(("gauge", name, value, tags))

    from airflow.sdk.observability import stats

    monkeypatch.setattr(stats, "Stats", FakeStats)
    _emit_metric("duration_ms", 12.5, {"variant": "fast", "attempt": 2})
    assert calls == [
        ("incr", "openfeature.outcome.duration_ms", {"variant": "fast", "attempt": "2"}),
        ("gauge", "openfeature.outcome.duration_ms.value", 12.5, {"variant": "fast", "attempt": "2"}),
    ]


def test_emit_metric_allows_count_only(monkeypatch):
    calls = []

    class FakeStats:
        @staticmethod
        def incr(name, tags=None):
            calls.append((name, tags))

        @staticmethod
        def gauge(name, value, tags=None):
            raise AssertionError("value metric should not be emitted")

    from airflow.sdk.observability import stats

    monkeypatch.setattr(stats, "Stats", FakeStats)
    _emit_metric("success", None, {"variant": "control"})
    assert calls == [("openfeature.outcome.success", {"variant": "control"})]


def test_emit_metric_is_best_effort(monkeypatch):
    class BadStats:
        @staticmethod
        def incr(name, tags=None):
            raise RuntimeError("stats backend down")

    from airflow.sdk.observability import stats

    monkeypatch.setattr(stats, "Stats", BadStats)
    _emit_metric("duration_ms", 12.5, {"variant": "fast"})
