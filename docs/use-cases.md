# Use cases

Concrete recipes. Each is a flag definition in your backend plus, for placement, the policy turned on
(`[openfeature] enable_policy = True`). Examples use flagd's JSON so they are copy-pasteable; the same
cohorts express as targeting rules or a percentage ramp in GrowthBook, Unleash, Statsig, or an
in-house engine. All four placement recipes run end to end in
[`../system_tests/run_use_cases.py`](../system_tests/run_use_cases.py).

## Platform progressive delivery (the policy)

The policy reads `airflow.task.pool`, `airflow.task.queue`, and `airflow.task.executor`, keyed on
`dag_id:task_id`, and applies whatever the backend returns.

### 1. Airflow 2→3 migration

Route a cohort of DAGs onto a 3.x worker pool, ramp, and roll back by flipping the flag.

```json
{
  "flags": {
    "airflow.task.pool": {
      "state": "ENABLED",
      "variants": { "v3": "airflow_3x", "v2": "airflow_2x" },
      "defaultVariant": "v2",
      "targeting": { "if": [ { "in": [ { "var": "dag_id" }, ["dag_000", "dag_001"] ] }, "v3", "v2" ] }
    }
  }
}
```

Widen the list (or switch to a `fractional` rule) to ramp; empty it to revert. No DAG edits.

### 2. Kubernetes worker migration

Move a cohort onto a Kubernetes queue gradually.

```json
{ "flags": { "airflow.task.queue": {
  "state": "ENABLED",
  "variants": { "k8s": "kubernetes", "celery": "default" },
  "defaultVariant": "celery",
  "targeting": { "if": [ { "fractionalEvaluation": [ { "var": "dag_id" },
    [ "k8s", 20 ], [ "celery", 80 ] ] } ] }
} } }
```

### 3. Executor rollout

Shift a cohort to `KubernetesExecutor` (Airflow 2.10+/3.x).

```json
{ "flags": { "airflow.task.executor": {
  "state": "ENABLED",
  "variants": { "k8s": "KubernetesExecutor", "default": "" },
  "defaultVariant": "default",
  "targeting": { "if": [ { "in": [ { "var": "dag_id" }, ["dag_000"] ] }, "k8s", "default" ] }
} } }
```

### 4. Kill switch / instant rollback

Because placement is a flag, reverting is a config change, not a deploy: set `defaultVariant` back to
the safe value (or disable the flag) and the next parse places every task on the default. This is the
backstop for the three rollouts above.

## Experimentation and A/B testing inside DAGs

Evaluate a flag for a stable entity inside a task, run the chosen branch, and let the exposure listener
record the assignment.

### 5. A/B a code path

```python
from openfeature_airflow.gate import variant

def choose_model(**context):
    entity = context["dag"].dag_id
    model = variant("ranking.model_version", entity, default="v1")
    return train(model)  # "v1" or "v2" per the backend's split
```

Define `ranking.model_version` as a weighted variant flag (say 90/10) in the backend to send 10% of
runs to `v2`. Enable the exposure listener to measure the two arms.

### 6. Wait for a rollout to reach a cohort

Gate a downstream task until a flag turns on for its DAG.

```python
from openfeature_airflow.sensors.feature_flag import FeatureFlagSensor

wait = FeatureFlagSensor(task_id="wait_for_rollout", flag_key="feature.new_path", conn_id="openfeature")
```

### 7. Gradual dependency or behavior rollout

Enable a new library, code path, or risky setting (for example disruption checkpointing) for a cohort
of runs before it becomes the default, using the same boolean gate as case 5.

## Other fits

- Per-tenant, per-team, or per-dataset feature gating (target on evaluation-context attributes).
- Cost control: route a cohort of heavy tasks to a cheaper pool with the `airflow.task.pool` recipe.
- Canary a scheduler or worker config to a small cohort, watch the exposure metric, then widen.
