"""Runtime provider metadata, exposed via the ``apache_airflow_provider`` entry point."""

from __future__ import annotations

from typing import Any

from openfeature_airflow import __version__


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "airflow-provider-openfeature",
        "name": "OpenFeature",
        "description": "Feature flags and progressive delivery for Airflow, via OpenFeature.",
        "versions": [__version__],
        "connection-types": [
            {
                "connection-type": "openfeature",
                "hook-class-name": "openfeature_airflow.hooks.openfeature.OpenFeatureHook",
            }
        ],
        "hooks": [
            {"integration-name": "OpenFeature", "python-modules": ["openfeature_airflow.hooks.openfeature"]}
        ],
        "sensors": [
            {
                "integration-name": "OpenFeature",
                "python-modules": ["openfeature_airflow.sensors.feature_flag"],
            }
        ],
        "config": {
            "openfeature": {
                "description": "OpenFeature integration settings.",
                "options": {
                    "enable_policy": {
                        "description": "Enable the flag-driven placement cluster policy "
                        "(overrides pool/queue/executor/priority_weight per task subset).",
                        "type": "boolean",
                        "default": "False",
                        "version_added": "0.1.0",
                        "example": None,
                    },
                    "enable_exposure_listener": {
                        "description": "Emit a group/variant exposure metric per task instance.",
                        "type": "boolean",
                        "default": "False",
                        "version_added": "0.1.0",
                        "example": None,
                    },
                },
            }
        },
    }

