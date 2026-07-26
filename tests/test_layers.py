from openfeature.evaluation_context import EvaluationContext

from openfeature_airflow.providers.fractional import BoolFlag, FractionalProvider

_ENTITIES = [f"dag_{i:03d}" for i in range(300)]


def _ctx(entity):
    return EvaluationContext(targeting_key=entity)


def test_same_layer_buckets_together():
    # two distinct flag keys sharing a layer randomize the same entities together
    p = FractionalProvider(bool_flags={
        "infra.executor.v2": BoolFlag(rollout_pct=50, layer="mig"),
        "infra.pool.v2": BoolFlag(rollout_pct=50, layer="mig"),
    })
    for e in _ENTITIES:
        a = p.resolve_boolean_details("infra.executor.v2", False, _ctx(e)).value
        b = p.resolve_boolean_details("infra.pool.v2", False, _ctx(e)).value
        assert a == b


def test_distinct_keys_are_orthogonal():
    # without a shared layer, two keys randomize independently and disagree on some entities
    p = FractionalProvider(bool_flags={
        "infra.executor.v2": BoolFlag(rollout_pct=50),
        "infra.pool.v2": BoolFlag(rollout_pct=50),
    })
    diffs = sum(
        p.resolve_boolean_details("infra.executor.v2", False, _ctx(e)).value
        != p.resolve_boolean_details("infra.pool.v2", False, _ctx(e)).value
        for e in _ENTITIES
    )
    assert diffs > 0


def test_layer_is_deterministic():
    p = FractionalProvider(bool_flags={"f": BoolFlag(rollout_pct=30, layer="L")})
    first = [p.resolve_boolean_details("f", False, _ctx(e)).value for e in _ENTITIES]
    second = [p.resolve_boolean_details("f", False, _ctx(e)).value for e in _ENTITIES]
    assert first == second
    assert 0 < sum(first) < len(_ENTITIES)  # roughly 30% enabled, not all-or-nothing
