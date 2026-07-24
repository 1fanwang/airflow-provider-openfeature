from __future__ import annotations

from unittest import mock

import pytest
from airflow.exceptions import AirflowNotFoundException
from openfeature import api

from openfeature_airflow.hooks import openfeature as openfeature_module
from openfeature_airflow.hooks.openfeature import OpenFeatureHook
from openfeature_airflow.providers.fractional import BoolFlag, FractionalProvider


@pytest.fixture(autouse=True)
def _clear_registered_cache():
    openfeature_module._REGISTERED_CONNECTIONS.clear()
    yield
    openfeature_module._REGISTERED_CONNECTIONS.clear()


class TestOpenFeatureHook:
    def test_uses_global_provider_when_connection_missing(self, monkeypatch):
        api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(100)}))
        monkeypatch.setattr(
            OpenFeatureHook, "get_connection", mock.Mock(side_effect=AirflowNotFoundException("missing"))
        )
        hook = OpenFeatureHook(conn_id="missing")
        assert hook.is_enabled("f", entity="e") is True
        assert hook.is_enabled("unknown", entity="e", default=False) is False
        assert hook.get_variant("unknown", entity="e", default="d") == "d"

    def test_registers_provider_from_connection_extra(self, monkeypatch):
        conn = mock.Mock()
        conn.extra_dejson = {
            "provider_class": "openfeature_airflow.providers.fractional.FractionalProvider",
            "provider_kwargs": {},
        }
        monkeypatch.setattr(OpenFeatureHook, "get_connection", mock.Mock(return_value=conn))
        with mock.patch.object(api, "set_provider") as set_provider:
            OpenFeatureHook(conn_id="c1").get_client()
        assert set_provider.called
        assert type(set_provider.call_args.args[0]).__name__ == "FractionalProvider"

    def test_provider_registered_only_once_per_connection(self, monkeypatch):
        conn = mock.Mock()
        conn.extra_dejson = {
            "provider_class": "openfeature_airflow.providers.fractional.FractionalProvider",
            "provider_kwargs": {},
        }
        get_connection = mock.Mock(return_value=conn)
        monkeypatch.setattr(OpenFeatureHook, "get_connection", get_connection)
        with mock.patch.object(api, "set_provider"):
            OpenFeatureHook(conn_id="c2").get_client()
            OpenFeatureHook(conn_id="c2").get_client()
        get_connection.assert_called_once()

