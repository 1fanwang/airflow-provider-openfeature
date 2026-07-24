from __future__ import annotations

from openfeature_airflow.provider_info import get_provider_info


def test_provider_info_shape():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-openfeature"
    assert info["name"] == "OpenFeature"

    ct = info["connection-types"][0]
    assert ct["connection-type"] == "openfeature"
    assert ct["hook-class-name"].endswith("OpenFeatureHook")

    opts = info["config"]["openfeature"]["options"]
    assert set(opts) == {"enable_policy", "enable_exposure_listener"}
    assert opts["enable_policy"]["type"] == "boolean"
    assert opts["enable_policy"]["default"] == "False"  # install must be a no-op by default
