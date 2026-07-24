from __future__ import annotations

import importlib
from pathlib import Path

import pytest

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


class TestEntryPointTargetsImport:
    """The three entry points Airflow auto-discovers must resolve to real objects."""

    def test_provider_info_entrypoint(self):
        from openfeature_airflow.provider_info import get_provider_info

        assert callable(get_provider_info)
        assert get_provider_info()["name"] == "OpenFeature"

    def test_policy_entrypoint_module_exposes_hookimpl(self):
        mod = importlib.import_module("openfeature_airflow.policy")
        assert hasattr(mod, "task_policy")

    def test_plugin_entrypoint_class(self):
        from airflow.plugins_manager import AirflowPlugin

        from openfeature_airflow.plugin import OpenFeaturePlugin

        assert issubclass(OpenFeaturePlugin, AirflowPlugin)


def test_pyproject_entrypoints_match_importable_targets():
    """Guard against pyproject entry points drifting from the code they point at."""
    tomllib = pytest.importorskip("tomllib")  # py3.11+; skipped on 3.10
    data = tomllib.loads(PYPROJECT.read_text())
    eps = data["project"]["entry-points"]

    assert eps["airflow.policy"]["openfeature"] == "openfeature_airflow.policy"
    importlib.import_module("openfeature_airflow.policy")

    module_path, _, attr = eps["apache_airflow_provider"]["provider_info"].partition(":")
    assert getattr(importlib.import_module(module_path), attr)

    module_path, _, attr = eps["airflow.plugins"]["openfeature"].partition(":")
    assert getattr(importlib.import_module(module_path), attr)
