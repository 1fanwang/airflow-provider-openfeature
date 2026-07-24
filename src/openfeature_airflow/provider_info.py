"""Runtime provider metadata, exposed via the ``apache_airflow_provider`` entry point."""

from __future__ import annotations

from typing import Any

from openfeature_airflow import __version__


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "airflow-provider-openfeature",
        "name": "OpenFeature",
        "description": "Evaluate feature flags and run progressive delivery in Airflow "
        "via the vendor-neutral OpenFeature API.",
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
                        "(overrides pool/queue/executor per task cohort).",
                        "type": "boolean",
                        "default": "False",
                        "version_added": "0.1.0",
                        "example": None,
                    },
                    "enable_exposure_listener": {
                        "description": "Emit a cohort/variant exposure metric per task instance.",
                        "type": "boolean",
                        "default": "False",
                        "version_added": "0.1.0",
                        "example": None,
                    },
                },
            }
        },
    }

