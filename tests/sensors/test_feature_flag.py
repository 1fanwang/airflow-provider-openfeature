from __future__ import annotations

from unittest import mock

import pytest
from airflow.exceptions import AirflowNotFoundException
from openfeature import api

from openfeature_airflow.hooks import openfeature as openfeature_module
from openfeature_airflow.hooks.openfeature import OpenFeatureHook
from openfeature_airflow.providers.fractional import BoolFlag, FractionalProvider
from openfeature_airflow.sensors.feature_flag import FeatureFlagSensor


@pytest.fixture(autouse=True)
def _no_connection(monkeypatch):
    openfeature_module._REGISTERED_CONNECTIONS.clear()
    monkeypatch.setattr(
        OpenFeatureHook, "get_connection", mock.Mock(side_effect=AirflowNotFoundException("missing"))
    )
    yield
    openfeature_module._REGISTERED_CONNECTIONS.clear()


class TestFeatureFlagSensor:
    def test_poke_true_when_flag_enabled(self):
        api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(100)}))
        sensor = FeatureFlagSensor(task_id="s", flag_key="f", entity="e")
        assert sensor.poke({}) is True

    def test_poke_false_when_flag_disabled(self):
        api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(0)}))
        sensor = FeatureFlagSensor(task_id="s", flag_key="f", entity="e")
        assert sensor.poke({}) is False

    def test_poke_respects_expected_false(self):
        api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(0)}))
        sensor = FeatureFlagSensor(task_id="s", flag_key="f", entity="e", expected=False)
        assert sensor.poke({}) is True

