"""Airflow plugin: the exposure listener plus an optional flag-placement UI panel."""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from openfeature_airflow import listener, ui


class OpenFeaturePlugin(AirflowPlugin):
    name = "openfeature"
    listeners = [listener]
    # Airflow 3.x UI panel, off unless AIRFLOW__OPENFEATURE__ENABLE_UI=True. No-op on Airflow 2.x.
    fastapi_apps = ui.fastapi_apps()
    external_views = ui.external_views()

