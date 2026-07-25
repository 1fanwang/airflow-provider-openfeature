# Example DAGs

Runnable DAGs for the main patterns. Start with [docs/getting-started.md](../docs/getting-started.md)
for the 5-minute walkthrough, then use these as templates. For a no-Airflow, no-Docker taste of the
deterministic ramp, run [`examples/quickstart.py`](../examples/quickstart.py).

Each DAG's docstring explains the scenario and the flag to define in your backend. The placement
examples need the policy enabled (`[openfeature] enable_policy = True`) and a backend registered; the
A/B example uses the in-task gate and needs neither.

| DAG | Pattern | Flag |
|---|---|---|
| [`openfeature_example.py`](openfeature_example.py) | Gate a task on a flag (sensor) + policy wiring | `airflow.example.rollout_ready` |
| [`migration_2to3_example.py`](migration_2to3_example.py) | Route a cohort of DAGs to a 3.x pool for a safe 2→3 migration | `airflow.task.pool` |
| [`kubernetes_executor_rollout_example.py`](kubernetes_executor_rollout_example.py) | Canary a KubernetesExecutor change ([apache/airflow#68480](https://github.com/apache/airflow/pull/68480)) by routing a cohort to a canary executor | `airflow.task.executor` |
| [`ab_test_model_example.py`](ab_test_model_example.py) | A/B a model version in a task, emit exposure for warehouse analysis | `ranking.model_version` |

The `system_tests/` directory has the end-to-end drivers that actually run these patterns against real
backends (flagd, GrowthBook, Unleash, Statsig) and read the result back from the metadata DB. See
[`system_tests/E2E.md`](../system_tests/E2E.md).
