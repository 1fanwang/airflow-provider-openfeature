from __future__ import annotations

from openfeature import api

from openfeature_airflow.policy import FLAG_POOL, FLAG_QUEUE, apply_placement
from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider


class _Task:
    def __init__(self, dag_id):
        self.dag_id = dag_id
        self.task_id = "t"
        self.pool = "default_pool"
        self.queue = "default"
        self.executor = None


POPULATION = [f"dag_{i:03d}" for i in range(20)]
CANARY = ["dag_000", "dag_001", "dag_002", "dag_003", "dag_004"]


def _place_all(provider):
    api.set_provider(provider)
    out = {}
    for d in POPULATION:
        t = _Task(d)
        apply_placement(t)
        out[d] = (t.pool, t.queue)
    return out


class TestPolicyEndToEndInProcess:
    """Real OpenFeature api + real policy + real provider, no external backend."""

    def test_targeting_places_exactly_the_subset(self):
        placements = _place_all(
            InHouseTreatmentProvider(
                string_flags={
                    FLAG_POOL: {
                        "segments": [{"attribute": "dag_id", "in": CANARY, "variant": "canary_pool"}],
                        "default": "default_pool",
                    },
                    FLAG_QUEUE: {
                        "segments": [{"attribute": "dag_id", "in": CANARY, "variant": "kubernetes"}],
                    },
                }
            )
        )
        in_canary_pool = {d for d, (pool, _) in placements.items() if pool == "canary_pool"}
        on_k8s_queue = {d for d, (_, queue) in placements.items() if queue == "kubernetes"}
        assert in_canary_pool == set(CANARY)
        assert on_k8s_queue == set(CANARY)

    def test_rollout_is_stable_across_runs(self):
        provider = InHouseTreatmentProvider(
            string_flags={FLAG_POOL: {"rollout": [("canary_pool", 25), ("default_pool", 75)]}}
        )
        assert _place_all(provider) == _place_all(provider)
