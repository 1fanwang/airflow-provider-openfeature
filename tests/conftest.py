from __future__ import annotations

import pytest
from openfeature import api


@pytest.fixture(autouse=True)
def _reset_openfeature_provider():
    """Isolate tests: clear any globally-registered OpenFeature provider after each test."""
    yield
    try:
        api.clear_providers()
    except Exception:
        pass
