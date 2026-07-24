from __future__ import annotations

from openfeature.evaluation_context import EvaluationContext

from openfeature_airflow.providers.fractional import (
    BoolFlag,
    FractionalProvider,
    VariantFlag,
    _bucket,
)


def _ctx(entity: str) -> EvaluationContext:
    return EvaluationContext(targeting_key=entity)


class TestFractionalProvider:
    def test_bucket_is_deterministic_and_in_range(self):
        assert _bucket("dag:t", "flag") == _bucket("dag:t", "flag")
        assert 0 <= _bucket("dag:t", "flag") < 100

    def test_boolean_matches_bucket_rule(self):
        provider = FractionalProvider(bool_flags={"f": BoolFlag(30)})
        for i in range(50):
            entity = f"dag_{i}"
            result = provider.resolve_boolean_details("f", False, _ctx(entity))
            assert result.value == (_bucket(entity, "f") < 30)

    def test_unknown_flag_returns_default(self):
        provider = FractionalProvider()
        assert provider.resolve_boolean_details("nope", True, _ctx("x")).value is True
        assert provider.resolve_string_details("nope", "d", _ctx("x")).value == "d"

    def test_boolean_distribution_tracks_percentage(self):
        provider = FractionalProvider(bool_flags={"f": BoolFlag(30)})
        n = 5000
        enabled = sum(provider.resolve_boolean_details("f", False, _ctx(f"d{i}")).value for i in range(n))
        assert abs(enabled / n - 0.30) < 0.03

    def test_variant_weights(self):
        provider = FractionalProvider(variant_flags={"m": VariantFlag([("a", 40), ("b", 60)])})
        n = 5000
        a = sum(provider.resolve_string_details("m", "?", _ctx(f"d{i}")).value == "a" for i in range(n))
        assert abs(a / n - 0.40) < 0.03

