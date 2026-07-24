from __future__ import annotations

from openfeature.evaluation_context import EvaluationContext

from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider, _bucket


def _ctx(entity, **attrs):
    return EvaluationContext(targeting_key=entity, attributes=attrs)


class TestInHouseTreatmentProvider:
    def test_segment_targeting(self):
        p = InHouseTreatmentProvider(
            string_flags={"f": {"segments": [{"attribute": "dag_id", "in": ["d1"], "variant": "on"}], "default": "off"}}
        )
        assert p.resolve_string_details("f", "x", _ctx("d1:t", dag_id="d1")).value == "on"
        assert p.resolve_string_details("f", "x", _ctx("d2:t", dag_id="d2")).value == "off"

    def test_rollout_is_deterministic(self):
        p = InHouseTreatmentProvider(string_flags={"f": {"rollout": [("a", 50), ("b", 50)]}})
        entity = "dag_5:t"
        expected = "a" if _bucket(entity, "f") < 50 else "b"
        assert p.resolve_string_details("f", "?", _ctx(entity)).value == expected

    def test_rollout_distribution(self):
        p = InHouseTreatmentProvider(string_flags={"f": {"rollout": [("a", 30), ("b", 70)]}})
        n = 5000
        a = sum(p.resolve_string_details("f", "?", _ctx(f"d{i}:t")).value == "a" for i in range(n))
        assert abs(a / n - 0.30) < 0.03

    def test_unknown_flag_returns_default(self):
        p = InHouseTreatmentProvider()
        assert p.resolve_string_details("nope", "d", _ctx("x")).value == "d"

    def test_boolean_from_variant(self):
        p = InHouseTreatmentProvider(
            string_flags={"f": {"segments": [{"attribute": "dag_id", "in": ["d1"], "variant": "on"}], "default": "off"}}
        )
        assert p.resolve_boolean_details("f", False, _ctx("d1:t", dag_id="d1")).value is True
        assert p.resolve_boolean_details("f", False, _ctx("d2:t", dag_id="d2")).value is False
