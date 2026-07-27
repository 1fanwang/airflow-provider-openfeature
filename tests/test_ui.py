"""Tests for the optional flag-placement UI panel wiring."""

from __future__ import annotations

from openfeature import api

from openfeature_airflow import ui
from openfeature_airflow.providers.fractional import FractionalProvider, VariantFlag


def test_hooks_are_off_by_default(monkeypatch):
    monkeypatch.setattr(ui, "_enabled", lambda: False)
    assert ui.fastapi_apps() == []
    assert ui.external_views() == []


def test_external_view_registered_when_enabled(monkeypatch):
    monkeypatch.setattr(ui, "_enabled", lambda: True)
    views = ui.external_views()
    assert views and views[0]["href"] == "/openfeature/" and views[0]["destination"] == "nav"


def test_state_shape_with_no_tasks(monkeypatch):
    monkeypatch.setattr(ui, "_real_tasks", lambda limit=500: [])
    monkeypatch.setattr(ui, "_provider_name", lambda: "TestProvider")
    state = ui._state()
    assert state == {
        "provider": "TestProvider",
        "total_tasks": 0,
        "moved_count": 0,
        "per_dimension": {},
        "moved": [],
    }


def test_state_reports_moved_tasks(monkeypatch):
    api.set_provider(
        FractionalProvider(
            variant_flags={"airflow.task.pool": VariantFlag([("canary_pool", 100), ("default_pool", 0)])}
        )
    )
    monkeypatch.setattr(ui, "_real_tasks", lambda limit=500: [("dag_a", "t1"), ("dag_a", "t2")])
    state = ui._state()
    assert state["total_tasks"] == 2
    assert state["moved_count"] == 2
    assert state["per_dimension"] == {"pool": 2}
    assert all(m["value"] == "canary_pool" and m["flag"] == "airflow.task.pool" for m in state["moved"])


def test_real_tasks_is_safe_without_airflow_db():
    # No Airflow metadata DB available in unit tests -> empty, never raises.
    assert ui._real_tasks() == []
