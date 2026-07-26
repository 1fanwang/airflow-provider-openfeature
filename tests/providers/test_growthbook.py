from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("growthbook")

from openfeature.evaluation_context import EvaluationContext  # noqa: E402

from openfeature_airflow.providers.growthbook import GrowthBookProvider  # noqa: E402


def _ctx(entity, **attrs):
    return EvaluationContext(targeting_key=entity, attributes=attrs)


class TestGrowthBookProvider:
    def test_features_targeting(self):
        features = {
            "airflow.task.pool": {
                "defaultValue": "default_pool",
                "rules": [{"condition": {"dag_id": {"$in": ["d1"]}}, "force": "canary_pool"}],
            }
        }
        p = GrowthBookProvider(features=features)
        assert p.resolve_string_details("airflow.task.pool", "x", _ctx("d1:t", dag_id="d1")).value == "canary_pool"
        assert p.resolve_string_details("airflow.task.pool", "x", _ctx("d2:t", dag_id="d2")).value == "default_pool"

    def test_requires_features_or_api_host(self):
        with pytest.raises(ValueError):
            GrowthBookProvider()

    def test_loads_features_from_api_host(self, monkeypatch):
        import growthbook

        seen = {}

        class FakeGrowthBook:
            def __init__(self, **kwargs):
                seen["kwargs"] = kwargs
                self.loaded = False

            def load_features(self):
                self.loaded = True
                seen["loaded"] = True

            def set_attributes(self, attrs):
                seen["attrs"] = attrs

            def eval_feature(self, flag_key):
                return SimpleNamespace(on=True, value="loaded")

        monkeypatch.setattr(growthbook, "GrowthBook", FakeGrowthBook)
        p = GrowthBookProvider(api_host="https://growthbook.example", client_key="sdk-key")
        assert seen == {"kwargs": {"api_host": "https://growthbook.example", "client_key": "sdk-key"}, "loaded": True}
        assert p.resolve_string_details("airflow.task.pool", "default", _ctx("dag")).value == "loaded"

    def test_eval_without_context_uses_anonymous_entity(self):
        p = GrowthBookProvider(features={"f": {"defaultValue": True}})
        assert p.resolve_boolean_details("f", False).value is True

    def test_integer_float_and_object_resolution(self):
        p = GrowthBookProvider(
            features={
                "int": {"defaultValue": "7"},
                "bad-int": {"defaultValue": "not-int"},
                "float": {"defaultValue": "1.5"},
                "bad-float": {"defaultValue": None},
                "object": {"defaultValue": {"pool": "canary"}},
                "missing": {"defaultValue": None},
            }
        )
        assert p.resolve_integer_details("int", 0, _ctx("dag")).value == 7
        assert p.resolve_integer_details("bad-int", 3, _ctx("dag")).value == 3
        assert p.resolve_float_details("float", 0.0, _ctx("dag")).value == 1.5
        assert p.resolve_float_details("bad-float", 2.5, _ctx("dag")).value == 2.5
        assert p.resolve_object_details("object", {}, _ctx("dag")).value == {"pool": "canary"}
        assert p.resolve_object_details("missing", {"pool": "default"}, _ctx("dag")).value == {"pool": "default"}

    def test_track_without_callback_is_noop(self):
        GrowthBookProvider(features={}).track("metric", _ctx("dag"))

    def test_track_callback_errors_are_swallowed(self):
        p = GrowthBookProvider(features={}, on_track=lambda *a: (_ for _ in ()).throw(RuntimeError("down")))
        p.track("metric", _ctx("dag"))
