from __future__ import annotations

from openfeature.evaluation_context import EvaluationContext

from openfeature_airflow.providers.fractional import (
    BoolFlag,
    FractionalProvider,
    VariantFlag,
    _bucket,
    entity_of,
)


def _ctx(entity: str) -> EvaluationContext:
    return EvaluationContext(targeting_key=entity)


class TestFractionalProvider:
    def test_bucket_is_deterministic_and_in_range(self):
        assert _bucket("dag:t", "flag") == _bucket("dag:t", "flag")
        assert 0 <= _bucket("dag:t", "flag") < 100

    def test_bucket_is_seed_independent(self):
        # Golden values: sha256 bucketing must not depend on PYTHONHASHSEED, so the same entity lands
        # in the same bucket across processes and restarts. A salted builtin hash() would drift and
        # fail this. Regenerate only if the bucketing algorithm intentionally changes.
        assert _bucket("dag_a:task_1", "airflow.task.pool") == 48
        assert _bucket("dag_a:task_2", "airflow.task.pool") == 25

    def test_cohort_is_monotonic(self):
        # A task in the cohort at a lower percentage stays in at a higher one, so raising a ramp only
        # adds entities and never reshuffles who was already in.
        entities = [f"dag_a:task_{i}" for i in range(500)]

        def in_at(pct):
            return {e for e in entities if _bucket(e, "airflow.task.pool") < pct}

        assert in_at(30) <= in_at(50) <= in_at(70)

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

    def test_entity_fallbacks(self):
        assert entity_of(None) == "__anonymous__"
        assert entity_of(EvaluationContext(attributes={"entity": "from-entity"})) == "from-entity"
        assert entity_of(EvaluationContext(attributes={"id": "from-id"})) == "from-id"

    def test_layer_shares_buckets_across_flags(self):
        provider = FractionalProvider(
            bool_flags={"first": BoolFlag(50, layer="shared"), "second": BoolFlag(50, layer="shared")}
        )
        for i in range(50):
            entity = _ctx(f"dag_{i}")
            assert (
                provider.resolve_boolean_details("first", False, entity).value
                == provider.resolve_boolean_details("second", False, entity).value
            )

    def test_variant_returns_default_when_weights_do_not_cover_bucket(self):
        provider = FractionalProvider(variant_flags={"m": VariantFlag([("a", 0)])})
        assert provider.resolve_string_details("m", "fallback", _ctx("dag")).value == "fallback"

    def test_non_string_resolvers_return_default(self):
        provider = FractionalProvider()
        assert provider.resolve_integer_details("i", 3, _ctx("dag")).value == 3
        assert provider.resolve_float_details("f", 1.5, _ctx("dag")).value == 1.5
        assert provider.resolve_object_details("o", {"a": 1}, _ctx("dag")).value == {"a": 1}
