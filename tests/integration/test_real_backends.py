"""Docker-backed integration test: real eval data from a live backend into a real task execution.

Skipped unless ``RUN_INTEGRATION=1`` (needs Docker for flagd plus an Airflow runtime), so CI's
unit job stays hermetic. Locally: ``RUN_INTEGRATION=1 pytest tests/integration -m integration``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]


@pytest.mark.skipif(os.environ.get("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1 (needs docker + airflow)")
def test_real_data_flow_drives_task_execution():
    """flagd/GrowthBook/Statsig over real network each flip a real TaskInstance's pool."""
    result = subprocess.run([sys.executable, str(REPO / "system_tests" / "real_data_flow.py")])
    assert result.returncode == 0
