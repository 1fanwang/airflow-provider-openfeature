<div align="center">

# airflow-provider-openfeature

**Feature flags for Apache Airflow.** Ramp a change across your DAGs, measure it, and revert with a
flag, not a redeploy.

[![CI](https://github.com/1fanwang/airflow-provider-openfeature/actions/workflows/ci.yml/badge.svg)](https://github.com/1fanwang/airflow-provider-openfeature/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

[![Python](https://img.shields.io/badge/python-3.9%E2%80%933.12-blue?logo=python&logoColor=white)](pyproject.toml)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.11%20%7C%203.3-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![OpenFeature](https://img.shields.io/badge/OpenFeature-provider-999?logo=openfeature&logoColor=white)](https://openfeature.dev)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

</div>

Feature flags let you control how your Airflow platform behaves at runtime, across many DAGs, without
editing them or redeploying. A platform team can move a cohort of tasks to a different pool, queue, or
executor, ramp a worker or executor migration, or flip a kill switch during an incident, centrally and
without touching anyone's DAG. A DAG author can gate a code path or A/B a model inside a task. Both go
through [OpenFeature](https://openfeature.dev), so it works with the flag backend you already run:
[flagd](https://flagd.dev), [LaunchDarkly](https://launchdarkly.com), [GrowthBook](https://www.growthbook.io),
[Unleash](https://www.getunleash.io), [Statsig](https://statsig.com), or an in-house engine.

<p align="center">
  <img src="docs/demo.svg" alt="Ramping the placement policy across 40 real Airflow tasks: dag_03's pool moves to the canary at 100%, then the kill switch reverts it" width="760">
</p>

## Try it in 30 seconds

No Docker, no backend, no running Airflow:

```bash
pip install airflow-provider-openfeature
python examples/quickstart.py
```

It builds 40 real Airflow tasks and runs the actual placement policy on them, moving each to a canary
pool as the flag ramps 0 → 100%, then reverting them on the kill switch. For the same thing against a
live backend and a running scheduler, follow the [5-minute getting-started](docs/getting-started.md).

## Why you need it

Feature flags are the standard way to change software behavior at runtime without shipping code. This
brings the same four moves to data pipelines:

- **Ramp, don't flip.** Roll a change out to 1% of runs, then 10%, then 100%, checking each step
  instead of switching everything at once.
- **Experiment.** A/B two implementations (a model version, a join strategy, a new library) across a
  cohort of runs and measure which wins, rather than guessing.
- **Kill switch.** Turn a misbehaving feature off during an incident with a flag change, not a redeploy.
- **Target.** Enable something for one team, tenant, or dataset before everyone else.

Those are the standard [toggle categories](https://martinfowler.com/articles/feature-toggles.html)
(release, experiment, ops, permission). The Airflow-specific part: the same flag can also move a task's
`pool`, `queue`, or `executor`, so you canary infrastructure (a worker migration, a new executor) the
same way you canary a feature. Container-canary tools like
[Argo Rollouts](https://argo-rollouts.readthedocs.io/en/stable/) and [Flagger](https://flagger.app/)
shift HTTP traffic between versions and, by their own docs, don't handle queue workers; Airflow
schedules from a pull queue, so a scheduler-level flag is how you ramp a cohort of DAGs.

## What you get

| | |
|---|---|
| **Move where and how tasks run** | A cluster policy reads a flag and sets a task's pool, queue, executor, or priority for a chosen subset of DAGs, at parse time. No DAG edits. |
| **Ramp and revert live** | Change a percentage in your backend. No redeploy, no scheduler restart. |
| **Any backend** | flagd, LaunchDarkly, GrowthBook, Statsig, Unleash, or an in-house engine, through OpenFeature. Swap without a rewrite. |
| **Measure the result** | One call records the outcome to your experiment platform (Statsig, GrowthBook) or your warehouse (OTEL/Grafana). |
| **Safe to install** | The policy and listener do nothing until you turn them on in config. |

## See it run

Airflow decides how each task runs from a few settings: its
[**pool**](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
(a named cap on how many tasks run at once), its
[**queue**](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/celery.html)
(which set of workers picks the task up), and its
[**executor**](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)
(whether the task runs on a shared worker or gets its own
[Kubernetes pod](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/kubernetes_executor.html)).
Normally you change these by editing DAGs or redeploying, and the change hits every DAG at once.

**Change them with a flag instead, for a cohort.** A *cohort* is just the subset you pick: by name, by
team, or by a percentage that you ramp. A
[cluster policy](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/cluster-policies.html)
reads the flag and applies the setting to that subset, so there is no rollout logic in the DAG itself:

```python
# a large fan-out you would rather move a few at a time (full DAG in example_dags/)
with DAG("features_pipeline", schedule=None, start_date=datetime(2024, 1, 1)) as dag:
    PythonOperator.partial(task_id="process", python_callable=build).expand(op_args=shards)
```

Say you want to try the KubernetesExecutor (each task runs as its own pod) on a handful of DAGs before
moving everything to it. Set `airflow.task.executor` to that executor for the cohort; those tasks run as
pods while the rest stay on the shared workers, and turning the flag off puts them back. No redeploy.
That is the safe way to adopt a change like [#68480](https://github.com/apache/airflow/pull/68480) (a
faster pod-creation path): the [case study](docs/case-study/) ramps it and measures how long tasks wait
to start, checking for a regression on a real cluster before widening.

**Measure the outcome, don't guess.** The exposure and the result flow back to your platform or
warehouse, so a rollout comes with a number. Here the same policy runs on real Airflow and the lift is
read back from each backend:

<p align="center">
  <img src="docs/demo-measure-loop.png" alt="Assign a cohort, run it, measure the outcome, and read the control-vs-treatment lift back" width="760">
</p>

**No vendor lock-in.** The same DAG and policy work on flagd, GrowthBook, Unleash, Statsig, or an
in-house engine through OpenFeature, so you use the backend you already run:

<p align="center">
  <img src="docs/demo-all-backends.gif" alt="The same policy running unchanged on flagd, GrowthBook, an in-house engine, Unleash, and Statsig" width="760">
</p>

Commands and raw output for all of this are in [`system_tests/E2E.md`](system_tests/E2E.md).

## Worked example: canary a pipeline change, end to end

A nightly `revenue_rollup` ETL is missing its SLA. An engineer has a faster rewrite of the aggregation
(`rollup_v2`), but it feeds finance dashboards, so shipping a wrong total to every region at once is not
an option. Put the rewrite behind a flag and ramp it across regions instead.

**In the DAG**, the task reads the flag through this provider, runs the chosen path, and records the
outcome. There is no rollout logic in the DAG; the cohort lives in the backend.

```python
from openfeature_airflow.gate import flag_enabled
from openfeature_airflow.measure import track_outcome

use_v2 = flag_enabled("revenue_rollup.use_fast_agg", region)      # the backend decides the cohort
result = (rollup_v2 if use_v2 else rollup_v1)(shard)              # run the chosen implementation
track_outcome("rollup_ms", region, value=elapsed_ms, variant="v2" if use_v2 else "v1")
```

**In the backend** (here Unleash), the rollout is a dial. Stickiness on the region key keeps a region
in its cohort as you raise the percentage. The provider reads any backend identically, so the same flag
drives it from GrowthBook, Statsig, or flagd with a one-line change.

<p align="center">
  <img src="docs/case-study/img/unleash-flag.png" alt="The revenue_rollup.use_fast_agg feature flag in the Unleash UI" width="49%">
  <img src="docs/case-study/img/unleash-rollout.png" alt="The gradual-rollout strategy set to 50% in the Unleash UI" width="49%">
</p>

**The result:** ramping 0 → 100% while Airflow reads the flag on each run, `v2` comes out about **89%
faster** and the revenue stays **identical to the cent** at every step. A wrong total would trip the
guardrail, and the fix would be one dial back to 0%, with no redeploy.

<p align="center">
  <img src="docs/case-study/img/run.png" alt="Ramping the revenue-rollup canary 0 to 100% across regions, 89% faster, revenue identical at every step" width="760">
</p>

The full walkthrough, plus a second example that evaluates the KubernetesExecutor async pod-creation
change ([#68480](https://github.com/apache/airflow/pull/68480)) for a queued-latency regression, is in
[**docs/case-study**](docs/case-study/).

## Examples

Runnable templates in [`example_dags/`](example_dags/); the patterns, mapped to the standard toggle
taxonomy, are in [docs/use-cases.md](docs/use-cases.md).

| Use case | What the flag does |
|---|---|
| [Canary a faster rollup](example_dags/revenue_rollup_dag.py) | run a rewritten aggregation for a cohort of regions, with a revenue-parity guardrail |
| [Airflow 2→3 migration](example_dags/migration_2to3_example.py) | route a cohort of DAGs onto a 3.x worker pool, ramp, roll back |
| [KubernetesExecutor canary](example_dags/kubernetes_executor_rollout_example.py) | shift a cohort to concurrent pod creation ([apache/airflow#68480](https://github.com/apache/airflow/pull/68480)), watch, widen |
| [A/B a model](example_dags/ab_test_model_example.py) | pick a model variant per run and emit the exposure + outcome |
| Worker / queue migration | move a cohort onto a Kubernetes queue gradually |
| Kill switch | revert placement for everyone with one flag change |

## Install

```bash
pip install airflow-provider-openfeature            # core
pip install "airflow-provider-openfeature[flagd]"   # + a backend, e.g. flagd
```

It is a standard PyPI package, so [uv](https://docs.astral.sh/uv/) works the same way:
`uv pip install airflow-provider-openfeature`.

Point OpenFeature at your backend once, in `airflow_local_settings.py` or a bootstrap:

```python
from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider

api.set_provider(FlagdProvider(host="localhost", port=8013))
```

Then turn on the piece you want (both default to off):

```ini
[openfeature]
enable_policy = True             # flag-driven pool/queue/executor placement
enable_exposure_listener = True  # record which cohort each run landed in
```

Bundled adapters for backends whose OpenFeature provider needs a nudge: `providers.growthbook`,
`providers.unleash`, `providers.statsig`, `providers.inhouse` (template for a proprietary engine), and
`providers.fractional` (dependency-free deterministic %-rollout for testing).
[flagd](https://flagd.dev), [LaunchDarkly](https://launchdarkly.com),
[Flagsmith](https://www.flagsmith.com) and [others](https://openfeature.dev/ecosystem/) ship their own
OpenFeature providers; use those directly.

## Gate a task, or measure an outcome

Evaluate a flag anywhere in a task:

```python
from openfeature_airflow.gate import flag_enabled

if flag_enabled("airflow.rollout.new_parser", dag_id):
    ...
```

Record the outcome for analysis (routes to Statsig, GrowthBook, LaunchDarkly, or your warehouse):

```python
from openfeature_airflow.measure import track_outcome

track_outcome("task_duration_ms", f"{dag_id}:{task_id}", value=elapsed_ms, variant=cohort)
```

See [docs/measurement.md](docs/measurement.md) for the per-backend readout.

## Docs

- [Getting started](docs/getting-started.md): a 5-minute walkthrough on real Airflow.
- [Case study](docs/case-study/): canary a faster pipeline step end to end, with a real Unleash backend.
- [Use cases](docs/use-cases.md): the toggle taxonomy mapped to Airflow, with recipes.
- [Measurement](docs/measurement.md): closing the loop with your experiment platform or warehouse.
- [Architecture](docs/architecture.md): the flow and the surfaces it registers.
- [Extending](docs/extending.md): add a backend.
- [Contributing](CONTRIBUTING.md) · [AGENTS.md](AGENTS.md).

These docs also build into a site: `pip install -e ".[docs]"` then `mkdocs serve` (or see
[`mkdocs.yml`](mkdocs.yml)); `.github/workflows/docs.yml` publishes it to GitHub Pages.

## How it works

Everything goes through the [OpenFeature evaluation API](https://openfeature.dev/specification/), so the
backend is a swap. The package adds three Airflow surfaces, auto-discovered via
[entry points](https://packaging.python.org/en/latest/specifications/entry-points/); the backend
decides who is in which cohort.

```mermaid
flowchart TB
    subgraph airflow["Airflow"]
        author["DAG / task code"]
        parse["parse & schedule<br/>(task_policy)"]
        done["task finished"]
    end
    subgraph pkg["openfeature_airflow"]
        hook["hook / sensor / gate"]
        policy["placement policy"]
        listener["exposure listener"]
    end
    api[["OpenFeature<br/>evaluation API"]]
    subgraph backends["Any OpenFeature backend"]
        flagd["flagd"]
        gb["GrowthBook"]
        unleash["Unleash"]
        inhouse["in-house engine"]
    end
    measure["warehouse /<br/>experiment platform"]

    author --> hook --> api
    parse --> policy --> api
    done --> listener --> api
    api --> flagd & gb & unleash & inhouse
    listener --> measure
```

It gives you two independent capabilities, both no-ops until you turn them on in config:

- **Placement policy.** A cluster policy consults a flag and overrides a task's `pool` / `queue` /
  `executor` / `priority_weight` for its cohort. A rollout is a backend config change, not a DAG edit.
- **In-DAG evaluation.** The hook, sensor, and gate read a flag for a stable entity inside a task, and
  the exposure listener records which cohort each run landed in, so you can measure it.

| Surface | Entry point | Purpose |
|---|---|---|
| `OpenFeatureHook`, `FeatureFlagSensor`, `openfeature` connection | `apache_airflow_provider` | evaluate a flag in a task |
| flag-driven placement policy | `airflow.policy` | override `pool`/`queue`/`executor`/`priority_weight` per cohort |
| exposure listener | `airflow.plugins` | emit the resolved cohort for measurement |

The policy reads these well-known flags, keyed on `dag_id:task_id`: `airflow.task.pool`,
`airflow.task.queue`, `airflow.task.executor`, `airflow.task.priority_weight`. The entity is bucketed
deterministically, so a task lands in the same cohort every parse until the backend config changes.
[`docs/architecture.md`](docs/architecture.md) has the step-by-step ramp and exposure sequence diagrams
and the Airflow version notes.

## Status

Alpha (0.1.0). The API may change before 1.0.

## License

Apache-2.0.
