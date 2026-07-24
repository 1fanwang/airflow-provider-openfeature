<div align="center">

# airflow-provider-openfeature

**Feature flags and progressive delivery for Apache Airflow, through the vendor-neutral [OpenFeature](https://openfeature.dev) API.**

[![CI](https://github.com/1fanwang/airflow-provider-openfeature/actions/workflows/ci.yml/badge.svg)](https://github.com/1fanwang/airflow-provider-openfeature/actions/workflows/ci.yml)
[![Publish](https://github.com/1fanwang/airflow-provider-openfeature/actions/workflows/publish.yml/badge.svg)](https://github.com/1fanwang/airflow-provider-openfeature/actions/workflows/publish.yml)
[![License](https://img.shields.io/github/license/1fanwang/airflow-provider-openfeature)](LICENSE)

[![Python](https://img.shields.io/badge/python-3.9%E2%80%933.12-blue?logo=python&logoColor=white)](pyproject.toml)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.11%20%7C%203.3-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![OpenFeature](https://img.shields.io/badge/OpenFeature-provider-999?logo=openfeature&logoColor=white)](https://openfeature.dev)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

[![Stars](https://img.shields.io/github/stars/1fanwang/airflow-provider-openfeature?style=flat)](https://github.com/1fanwang/airflow-provider-openfeature/stargazers)
[![Forks](https://img.shields.io/github/forks/1fanwang/airflow-provider-openfeature?style=flat)](https://github.com/1fanwang/airflow-provider-openfeature/network/members)
[![Issues](https://img.shields.io/github/issues/1fanwang/airflow-provider-openfeature)](https://github.com/1fanwang/airflow-provider-openfeature/issues)
[![Pull requests](https://img.shields.io/github/issues-pr/1fanwang/airflow-provider-openfeature)](https://github.com/1fanwang/airflow-provider-openfeature/pulls)

</div>

Evaluate feature flags in DAGs and run **progressive delivery of the platform** (canary, blue-green,
gradual rollout of pools, queues, and behavior) against any backend: flagd, GrowthBook, Unleash,
Statsig, or an in-house engine, through one vendor-neutral API. Installing changes nothing until you
opt in.

> A third-party provider, not part of the Apache Airflow monorepo.

<p align="center">
  <img src="docs/demo.svg" alt="Ramping a canary pool from 0 to 100 percent across 40 DAGs, then a kill-switch rollback" width="760">
</p>

## Contents

- [Try it](#try-it)
- [Why it's useful](#why-its-useful)
- [Use cases](#use-cases)
- [Architecture](#architecture)
- [What it registers](#what-it-registers-auto-discovered-via-entry-points)
- [Install](#install) · [Register a backend](#register-a-backend)
- [Progressive delivery in one flag](#progressive-delivery-in-one-flag)
- [Proven across backends](#proven-across-backends-with-real-data-flow) · [Docs](#documentation)

## Try it

No Docker, no backend, no running Airflow:

```bash
pip install airflow-provider-openfeature
python examples/quickstart.py
```

It ramps a canary pool from 0% to 100% across 40 DAGs with the deterministic bucketing the policy
uses, then flips the flag back to 0 (the kill switch). That is the demo above.

## Why it's useful

Two problems, one flag API.

**Rolling out platform changes is risky and slow.** Migrating workers, moving to a new executor, or
turning on a new behavior usually means editing DAGs or redeploying, and a bad change hits everything
at once. A cluster policy here consults a flag to place each task's `pool` / `queue` / `executor` by
cohort, so a rollout becomes a backend config change: ramp 1% → 100%, watch, and revert instantly by
flipping the flag. No DAG edits, no redeploy. Deployment tools like Argo Rollouts or Flagger gate
containers; they can't express a cohort keyed by `dag_id`, `pool`, or `queue`. A flag can.

**Experimenting inside a DAG means hand-rolling config plumbing.** The hook, sensor, and code-path
gate let a task read a flag for a deterministic entity, and the exposure listener records which
cohort each run landed in, so the result is measurable in whatever experimentation platform you
already use.

Both halves speak [OpenFeature](https://openfeature.dev), so the backend (flagd, GrowthBook, Unleash,
LaunchDarkly, an in-house engine) is a swap, not a rewrite.

## Use cases

**Progressive delivery of the platform** (the policy), proven end to end in
[`system_tests/`](system_tests/):

- **Airflow 2→3 migration.** Route a cohort of DAGs onto a 3.x worker pool, ramp the percentage, roll
  back by flipping the flag.
- **Worker / infra migration.** Move a cohort onto a Kubernetes queue gradually instead of all at once.
- **Executor rollout.** Shift a cohort to `KubernetesExecutor` and watch before widening.
- **Priority / SLA tuning.** Raise `priority_weight` for a cohort during a backfill or an incident.
- **Cost control.** Send a cohort of heavy tasks to a cheaper pool.
- **Kill switch.** Revert placement for everyone with one flag change, no deploy
  ([`kill_switch.py`](system_tests/kill_switch.py)).

**Experimentation and A/B testing inside DAGs** (the hook / sensor / gate):

- A/B a model version or algorithm branch inside a task, keyed on a stable entity, with exposure
  emitted for analysis ([`ab_experiment.py`](system_tests/ab_experiment.py)).
- Roll a new library or code path out to a cohort of runs before making it the default.
- Gate a feature per tenant, per team, or per dataset.

The cohort logic lives in the backend, so none of these need a code change to ramp or revert.

## Architecture

Everything routes through the vendor-neutral OpenFeature evaluation API, so the backend is a swap.
The package adds three Airflow surfaces on top; the backend decides who is in which cohort.

```mermaid
flowchart LR
    author["DAG author code"] --> hook["OpenFeatureHook / sensor / gate"]
    policy["Cluster policy<br/>(task_policy)"] --> place["placement policy"]
    runs["Completed task"] --> listen["exposure listener"]
    hook --> API[["OpenFeature<br/>evaluation API"]]
    place --> API
    API --> flagd & GrowthBook & Unleash & inhouse["in-house engine"]
    listen --> measure["exposure to<br/>warehouse / experiment platform"]
```

See [`docs/architecture.md`](docs/architecture.md) for the placement and evaluation sequence diagrams.

## What it registers (auto-discovered via entry points)

| Surface | Entry point | Purpose |
|---|---|---|
| `OpenFeatureHook`, `FeatureFlagSensor`, `openfeature` connection | `apache_airflow_provider` | evaluate a flag in a task; configure the backend |
| flag-driven placement policy | `airflow.policy` | override `pool`/`queue`/`executor`/`priority_weight` per cohort (**the progressive-delivery half**) |
| exposure listener | `airflow.plugins` | emit the resolved cohort/variant for measurement |

The policy and listener are **no-ops until enabled** in config, so `pip install` is safe:

```ini
[openfeature]
enable_policy = True             # turn on flag-driven placement
enable_exposure_listener = True  # emit cohort exposure metrics
```

## Install

```bash
pip install airflow-provider-openfeature            # core
pip install "airflow-provider-openfeature[flagd]"   # + the flagd provider
pip install "airflow-provider-openfeature[growthbook]"
pip install "airflow-provider-openfeature[unleash]"
```

## Register a backend

Point OpenFeature at any provider once, in `airflow_local_settings.py` or a bootstrap:

```python
from openfeature import api

# self-hosted flagd
from openfeature.contrib.provider.flagd import FlagdProvider
api.set_provider(FlagdProvider(host="localhost", port=8013))

# or a bundled adapter
from openfeature_airflow.providers.growthbook import GrowthBookProvider
api.set_provider(GrowthBookProvider(features=...))
```

Bundled adapters: `providers.growthbook`, `providers.unleash`, `providers.inhouse` (template for a
proprietary engine), and `providers.fractional` (dependency-free deterministic %-rollout for testing).
flagd, LaunchDarkly, Flagsmith and others ship their own OpenFeature providers; use those directly.

## Progressive delivery in one flag

Enable the policy, then define the flag in your backend. The policy reads these well-known flags,
keyed on `dag_id:task_id`, and applies each one that is set:

- `airflow.task.pool` → `task.pool`
- `airflow.task.queue` → `task.queue`
- `airflow.task.executor` → `task.executor`
- `airflow.task.priority_weight` → `task.priority_weight` (integer)

Ramp `airflow.task.pool` from 1% to 100% in the backend and a cohort of tasks moves to a canary pool
with no code change. Author-facing evaluation (gate a single task) uses the hook or sensor instead.

## Proven across backends, with real data flow

The same DAG population and policy gate byte-identically across flagd, GrowthBook, Unleash, Statsig,
and an in-house engine. Beyond parse-time gating, real eval data flows over the network into real task
execution: in [`system_tests/real_data_flow.py`](system_tests/real_data_flow.py), a cohort config
fetched from a live backend (flagd over gRPC, GrowthBook over HTTP, Statsig over HTTP) decides which
pool a real `airflow dags test` task actually runs in, read back from the metadata DB. Full commands
and raw output are in [`system_tests/E2E.md`](system_tests/E2E.md). Run it:

```bash
docker run -d --name flagd-e2e -p 8013:8013 -v "$PWD/system_tests/flags/flags.json:/etc/flagd/flags.json" \
  ghcr.io/open-feature/flagd:latest start --uri file:/etc/flagd/flags.json
docker compose -f system_tests/docker-compose.unleash.yml up -d
PYTHONPATH="$PWD/src:$PWD/system_tests" python system_tests/run_all_backends.py   # identical gating
python system_tests/real_data_flow.py                                            # live data -> real task run
```

## Documentation

- [Architecture](docs/architecture.md): the mental model and flow diagrams.
- [Use cases](docs/use-cases.md): migration routing, worker/executor rollout, A/B testing, kill switch.
- [Extending](docs/extending.md): add a backend, and how the ecosystem plugs in.
- [Contributing](CONTRIBUTING.md) · [AGENTS.md](AGENTS.md): dev setup, invariants, test commands.

## Status

Alpha (0.1.0). The API may change before 1.0.

## License

Apache-2.0.
