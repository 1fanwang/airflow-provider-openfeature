"""Airflow plugin that registers the exposure listener (a no-op until enabled in config)."""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from openfeature_airflow import listener


class OpenFeaturePlugin(AirflowPlugin):
    name = "openfeature"
    listeners = [listener]

