from __future__ import annotations

from openfeature_airflow import listener
from openfeature_airflow.plugin import OpenFeaturePlugin


def test_plugin_name_and_listener():
    from airflow.plugins_manager import AirflowPlugin

    assert issubclass(OpenFeaturePlugin, AirflowPlugin)
    assert OpenFeaturePlugin.name == "openfeature"
    assert listener in OpenFeaturePlugin.listeners
