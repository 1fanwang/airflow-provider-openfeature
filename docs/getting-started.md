# Getting started

By the end of this walkthrough you will:

- install the provider,
- point Airflow at a local [flagd](https://flagd.dev) feature-flag daemon,
- watch a flag move a DAG's task onto a canary pool,
- ramp that flag from a couple of DAGs to all of them,
- flip a kill switch and watch every DAG revert.

No experimentation-platform account needed. Everything runs locally against the open-source flagd
daemon. About five minutes.

## 0. Prerequisites

- Python 3.10+ and `pip`
- Docker (to run flagd)

## 1. Install

```bash
pip install "airflow-provider-openfeature[flagd]"
```

Installing changes nothing about your Airflow until you opt in (steps 3). The policy and listener stay
off by default.

## 2. Start flagd with one flag

Create a flag file. It routes two DAGs (`etl_alpha`, `etl_beta`) to a `canary_pool`; everything else
stays on the default.

```bash
mkdir -p flags && cat > flags/flags.json <<'JSON'
{
  "flags": {
    "airflow.task.pool": {
      "state": "ENABLED",
      "variants": { "canary": "canary_pool", "default": "default_pool" },
      "defaultVariant": "default",
      "targeting": { "if": [ { "in": [ {"var": "dag_id"}, ["etl_alpha", "etl_beta"] ] }, "canary", "default" ] }
    }
  }
}
JSON

docker run -d --name flagd -p 8013:8013 -v "$PWD/flags:/flags" \
  ghcr.io/open-feature/flagd:latest start --uri file:/flags/flags.json
```

flagd watches that file and hot-reloads on change. That's what makes the ramp in step 6 instant.

## 3. Point Airflow at flagd, and turn the policy on

Two things: register flagd as the OpenFeature backend, and enable the placement policy.

Register the backend in `$AIRFLOW_HOME/config/airflow_local_settings.py`:

```python
import time
from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider

api.set_provider(FlagdProvider(host="localhost", port=8013))
time.sleep(1)  # the gRPC resolver needs a moment to connect
```

Enable the policy (it ships off; this is the opt-in):

```bash
export AIRFLOW__OPENFEATURE__ENABLE_POLICY=True
```

You do not write a `task_policy` function. The `airflow.policy` entry point registers it for you; the
config flag turns it on.

## 4. Add a DAG

```python
# $AIRFLOW_HOME/dags/etl_alpha.py
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
import datetime

with DAG(dag_id="etl_alpha", schedule=None, start_date=datetime.datetime(2024, 1, 1), catchup=False) as dag:
    EmptyOperator(task_id="run", pool="default_pool")
```

The task asks for `default_pool`. The flag will override it.

## 5. Run it, and read the placement back

```bash
airflow dags test etl_alpha
```

The task ran in `canary_pool`, not the `default_pool` the DAG asked for. flagd put `etl_alpha` in the
canary subset; the policy applied it. Confirm from the metadata DB:

```bash
airflow tasks states-for-dag-run etl_alpha <run_id>   # pool column shows canary_pool
```

A DAG that isn't in the subset (say `etl_gamma`) runs in `default_pool`. Same code, different placement.

## 6. Ramp it

Widen the subset in the flag file. flagd hot-reloads; no restart, no redeploy.

```bash
# add more DAGs to the "in" list, or switch to a percentage with flagd's fractional operator:
#   "targeting": { "fractional": [ ["canary", 25], ["default", 75] ] }
```

Re-run any DAG and more of them land on `canary_pool`. The assignment is deterministic, so a DAG that
was canary at 25% stays canary at 50%.

## 7. Kill switch

Something looks wrong. Empty the subset:

```json
"targeting": { "if": [ { "in": [ {"var": "dag_id"}, [] ] }, "canary", "default" ] }
```

flagd reloads, and the next run of every DAG is back on `default_pool`. One flag change, no code edit,
no deploy.

## What just happened

The policy runs at DAG-parse time. For each task it asks OpenFeature to resolve `airflow.task.pool`,
keyed on the task's `dag_id:task_id`, and applies whatever comes back. flagd decided who was in the
subset. Swap flagd for GrowthBook, Unleash, Statsig, or an in-house engine and nothing in your Airflow
changes; the backend just answers the same question.

The policy reads three more well-known flags the same way: `airflow.task.queue`,
`airflow.task.executor`, and `airflow.task.priority_weight`. Set any of them to route a subset's queue,
executor, or priority.

## Next

- [Use cases](use-cases.md): 2→3 migration, KubernetesExecutor rollout, A/B testing, kill switch, cost routing.
- [Examples](../examples/): runnable DAGs for each pattern.
- [Register a backend](../README.md#register-a-backend): GrowthBook, Unleash, Statsig, or your own.
- Clean up: `docker rm -f flagd`.
