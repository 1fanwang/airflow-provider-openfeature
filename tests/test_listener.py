from __future__ import annotations

from openfeature import api

from openfeature_airflow import listener
from openfeature_airflow.policy import FLAG_POOL, FLAG_QUEUE
from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider


def _pool_provider(canary):
    api.set_provider(
        InHouseTreatmentProvider(
            string_flags={
                FLAG_POOL: {
                    "segments": [{"attribute": "dag_id", "in": canary, "variant": "canary_pool"}],
                    "default": "default_pool",
                }
            }
        )
    )


class _TI:
    def __init__(self, dag_id="d1", task_id="t", run_id="r"):
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id


class TestEmitExposure:
    def test_returns_resolved_group(self):
        _pool_provider(["d1"])
        assert listener.emit_exposure("d1", "t")[FLAG_POOL] == "canary_pool"

    def test_records_default_group(self):
        _pool_provider(["d1"])
        assert listener.emit_exposure("d2", "t")[FLAG_POOL] == "default_pool"

    def test_omits_unconfigured_flags(self):
        _pool_provider(["d1"])
        out = listener.emit_exposure("d1", "t")
        assert FLAG_QUEUE not in out and "airflow.task.executor" not in out


class TestListenerHook:
    def test_noop_when_disabled(self, monkeypatch):
        monkeypatch.setattr(listener, "_enabled", lambda: False)
        calls = []
        monkeypatch.setattr(listener, "emit_exposure", lambda *a, **k: calls.append(1))
        listener.on_task_instance_running(None, _TI(), None)
        assert not calls

    def test_emits_when_enabled(self, monkeypatch):
        monkeypatch.setattr(listener, "_enabled", lambda: True)
        seen = {}
        monkeypatch.setattr(listener, "emit_exposure", lambda d, t, r=None, **k: seen.update(dag=d, task=t, run=r))
        listener.on_task_instance_running(None, _TI(), None)
        assert seen == {"dag": "d1", "task": "t", "run": "r"}

    def test_never_raises_on_bad_input(self, monkeypatch):
        monkeypatch.setattr(listener, "_enabled", lambda: True)
        listener.on_task_instance_running(None, None, None)  # no task_instance -> no error
