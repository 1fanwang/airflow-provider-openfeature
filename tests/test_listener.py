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

    def test_enabled_reads_airflow_config(self, monkeypatch):
        from airflow.configuration import conf

        monkeypatch.setattr(conf, "getboolean", lambda section, key, fallback=False: (section, key, fallback))
        assert listener._enabled() == ("openfeature", "enable_exposure_listener", False)

    def test_emits_stats_metric_for_each_treatment(self, monkeypatch):
        calls = []

        class FakeStats:
            @staticmethod
            def incr(name, tags=None):
                calls.append((name, tags))

        from airflow.sdk.observability import stats

        monkeypatch.setattr(stats, "Stats", FakeStats)
        _pool_provider(["d1"])
        listener.emit_exposure("d1", "t", "run-1", flags=(FLAG_POOL,))
        assert calls == [
            ("openfeature.exposure", {"flag": FLAG_POOL, "variant": "canary_pool", "dag_id": "d1"})
        ]

    def test_metric_errors_do_not_break_exposure(self, monkeypatch):
        class BadStats:
            @staticmethod
            def incr(name, tags=None):
                raise RuntimeError("stats backend down")

        from airflow.sdk.observability import stats

        monkeypatch.setattr(stats, "Stats", BadStats)
        _pool_provider(["d1"])
        assert listener.emit_exposure("d1", "t", flags=(FLAG_POOL,)) == {FLAG_POOL: "canary_pool"}


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

    def test_logs_and_swallows_exposure_errors(self, monkeypatch, caplog):
        monkeypatch.setattr(listener, "_enabled", lambda: True)
        monkeypatch.setattr(listener, "emit_exposure", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        with caplog.at_level("DEBUG", logger=listener.log.name):
            listener.on_task_instance_running(None, _TI(), None)
        assert "openfeature exposure listener skipped: boom" in caplog.text
