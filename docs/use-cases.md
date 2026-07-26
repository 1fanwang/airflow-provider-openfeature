# Use cases

Feature flags fall into a few well-known categories ([Martin Fowler's toggle taxonomy](https://martinfowler.com/articles/feature-toggles.html),
[Unleash's flag types](https://docs.getunleash.io/concepts/feature-flags)). This maps them to Airflow,
where the flag targets a set of DAGs or tasks and the lever is a pool, queue, executor, priority, or a
code path.

| Category | In Airflow | Lever |
|---|---|---|
| Release / rollout | Ramp a risky platform change to a subset of DAGs, then widen | `pool` / `queue` / `executor` |
| Experiment | A/B a model, query, or algorithm inside a task | in-task gate + exposure |
| Ops / kill switch | Revert instantly during an incident, no redeploy | any flag, flipped off |
| Permission | Gate a feature per team, tenant, or dataset | gate on context attributes |

**Why not deployment canary ([Argo Rollouts](https://argo-rollouts.readthedocs.io/en/stable/), [Flagger](https://flagger.app/))?** Those shift HTTP traffic between container
versions and [do not support queue workers](https://argo-rollouts.readthedocs.io/en/stable/concepts/).
Airflow schedules from a pull queue, so they can't express "route these DAGs to the canary pool." A
flag evaluated in the scheduler policy can, and it reverts in seconds instead of a redeploy cycle.

New here? Walk through [getting-started.md](getting-started.md) first. Runnable versions of these are
in [`../example_dags/`](https://github.com/1fanwang/airflow-provider-openfeature/tree/main/example_dags); the end-to-end drivers are in
[`../system_tests/`](https://github.com/1fanwang/airflow-provider-openfeature/tree/main/system_tests).

## Placement (the policy)

Enable the policy (`[openfeature] enable_policy = True`) and register a backend. The policy reads
`airflow.task.pool`, `airflow.task.queue`, `airflow.task.executor`, and `airflow.task.priority_weight`,
keyed on `dag_id:task_id`, and applies whatever the backend returns. The JSON below is flagd's; the
same subset expresses as a targeting rule or a percentage ramp in GrowthBook, Unleash, Statsig, or an
in-house engine.

### 1. Airflow 2 to 3 migration

Migrating every DAG at once is unsafe, so Airflow's guidance is to stand up a 3.x worker pool and move
DAGs onto it a few at a time. Route the subset with a flag; ramp by widening it, revert by emptying
it. No DAG edits.

```json
{ "flags": { "airflow.task.pool": {
  "state": "ENABLED",
  "variants": { "v3": "airflow_3x", "v2": "airflow_2x" },
  "defaultVariant": "v2",
  "targeting": { "if": [ { "in": [ { "var": "dag_id" }, ["etl_alpha", "etl_beta"] ] }, "v3", "v2" ] }
} } }
```

### 2. Kubernetes worker / queue migration

Move a subset onto a Kubernetes queue with a percentage ramp. flagd's `fractional` operator does
deterministic sticky bucketing, so a DAG that's in the subset at 20% stays in it at 50%.

```json
{ "flags": { "airflow.task.queue": {
  "state": "ENABLED",
  "variants": { "k8s": "kubernetes", "celery": "default" },
  "defaultVariant": "celery",
  "targeting": { "fractional": [ ["k8s", 20], ["celery", 80] ] }
} } }
```

### 3. Canary a KubernetesExecutor change

[KubernetesExecutor](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/kubernetes_executor.html)
creates pods serially in the scheduler loop; [apache/airflow#68480](https://github.com/apache/airflow/pull/68480)
adds opt-in concurrent pod creation. A change like that wants a canary. Enable it on a canary KubernetesExecutor,
route a subset there with `airflow.task.executor`, watch `queued_duration`, and flip the flag off to
revert. Per-task `executor` needs Airflow 3.x (or 2.10+ with multiple executors configured).

```json
{ "flags": { "airflow.task.executor": {
  "state": "ENABLED",
  "variants": { "canary": "KubernetesExecutorCanary", "default": "" },
  "defaultVariant": "default",
  "targeting": { "if": [ { "in": [ { "var": "dag_id" }, ["etl_alpha"] ] }, "canary", "default" ] }
} } }
```

### 4. Priority / SLA and cost

Raise `airflow.task.priority_weight` for a subset during a backfill or an incident, or route a subset
of heavy tasks to a cheaper pool with the `airflow.task.pool` recipe.

### 5. Kill switch

Placement is a flag, so reverting is a config change, not a deploy: empty the subset or disable the
flag and the next parse puts every task back on the default. This is what makes the rollouts above
safe. [Knight Capital lost $460M in 45 minutes](https://en.wikipedia.org/wiki/Knight_Capital_Group#2012_stock_trading_disruption)
from a change it had no way to switch off; [DORA's 2021 report](https://dora.dev/research/2021/)
ties elite incident recovery to exactly this kind of instant, deploy-free control.

## Experiment (the gate + exposure)

Evaluate a flag for a stable entity inside a task, run the chosen branch, and let the exposure listener
record the assignment. This matches how experimentation platforms work: assign, emit an exposure event,
and measure downstream in your warehouse.

### 6. A/B a model or algorithm

```python
from openfeature_airflow.gate import variant

def choose_model(**context):
    entity = context["dag"].dag_id
    return variant("ranking.model_version", entity, default="v1")  # "v1" or "v2" per the split
```

Define `ranking.model_version` as a weighted variant (say 90/10) to send 10% of runs to `v2`. Enable
the exposure listener to record the arm for analysis. Full example:
`example_dags/ab_test_model_example.py`.

### 7. Wait for a rollout to reach a subset

Hold a downstream task until a flag turns on for its DAG.

```python
from openfeature_airflow.sensors.feature_flag import FeatureFlagSensor

wait = FeatureFlagSensor(task_id="wait_for_rollout", flag_key="feature.new_path", conn_id="openfeature")
```

### 8. Gradual dependency or behavior rollout

Enable a new library, code path, or risky setting for a subset of runs before it becomes the default,
using the same boolean gate as case 6.

### 9. AI and agent pipelines

Model training, batch inference, and LLM/agent workflows on Airflow are where flag-driven experiments
pay off most, since the whole point of the pipeline is to compare versions and measure the result:

- **A/B a model, prompt, or retrieval strategy** across a subset of runs (case 6), then read which wins
  on accuracy, latency, or cost from the exposure plus outcome, instead of shipping a guess.
- **Ramp a new agent tool or reasoning path** (case 8) to a fraction of runs before it becomes the default.
- **Kill switch an expensive or misbehaving model call** (case 5) the moment cost or quality drifts, with
  no redeploy.
- **Trade cost for quality by subset** by routing some runs to a smaller model and measuring the gap with
  `track_outcome`, so the tradeoff is a number rather than an argument.

Assignment and measurement here are the same primitives as any other experiment, so an AI pipeline gets
the assign → expose → measure loop without standing up a separate experimentation stack.

## Permission

Gate a feature per team, tenant, or dataset by targeting on evaluation-context attributes: pass them as
`**attributes` to the gate, or read them from the task in the policy.
