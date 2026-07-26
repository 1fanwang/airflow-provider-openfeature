from __future__ import annotations

from types import SimpleNamespace

from openfeature_airflow import _quickstart


def test_build_tasks_creates_real_default_pool_tasks():
    tasks = _quickstart.build_tasks()
    assert len(tasks) == 40
    assert {task.task_id for task in tasks} == {"run"}
    assert {task.pool for task in tasks} == {"default_pool"}


def test_place_moves_all_tasks_at_full_rollout():
    tasks = [SimpleNamespace(dag_id=f"dag_{i}", task_id="run", pool="default_pool") for i in range(5)]
    assert _quickstart.place(tasks, 100) == 5
    assert {task.pool for task in tasks} == {"canary_pool"}


def test_bar_renders_filled_and_empty_segments():
    assert _quickstart.bar(10, total=40, width=8) == "██░░░░░░"


def test_main_prints_ramp_and_rollback(monkeypatch, capsys):
    tasks = [SimpleNamespace(pool="default_pool") for _ in range(4)]

    def fake_place(all_tasks, percent):
        all_tasks[3].pool = "canary_pool" if percent else "default_pool"
        return percent // 25

    monkeypatch.setattr(_quickstart, "build_tasks", lambda: tasks)
    monkeypatch.setattr(_quickstart, "place", fake_place)
    monkeypatch.setattr(_quickstart.time, "sleep", lambda delay: None)
    _quickstart.main()
    out = capsys.readouterr().out
    assert "ramping airflow.task.pool across 4 real Airflow tasks" in out
    assert "flag 100%" in out
    assert "kill switch" in out
