from __future__ import annotations

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
