# AGENTS.md

Guide for agents and contributors on `airflow-provider-openfeature`. Read this first, and keep it
current (see [Keep everything in sync](#keep-everything-in-sync)).

## The vision

Bring progressive delivery to Apache Airflow's runtime. Web and mobile teams have shipped behind feature
flags for years: ramp a change from 1% to 100%, measure it, revert in seconds, all without a deploy.
Data pipelines never got that. This project gives Airflow the same control, through the open standard
([OpenFeature](https://openfeature.dev)) rather than one vendor, so a platform team can change how and
where pipelines run without editing anyone's DAG or redeploying.

It stays deliberately small: a pip package that plugs into two extension points Airflow and OpenFeature
already expose. It does not fork Airflow, patch the scheduler, or ask authors to rewrite DAGs, and it
does nothing until it is switched on in config.

## Who it's for, and the use cases

The primary user is a **platform or infrastructure team** running Airflow for many teams, where changing
task routing today means a DAG edit, a review, and a deploy. The recurring jobs, stated generally:

- **Ramp an infrastructure change to a subset.** Move a slice of tasks to a new executor, worker pool,
  or a faster code path; watch it; widen it; revert at once.
- **Kill-switch during an incident.** Turn a misbehaving feature or DAG family off with a flag change,
  not a redeploy.
- **Target before rollout.** Enable something for one team, tenant, or dataset first.
- **Experiment and measure.** A/B a model, a join strategy, or a library inside a task and record the
  exposure and outcome for a real comparison.

The secondary user is a **DAG author** who wants to gate a code path or A/B a choice inside a task, with
one call and no platform access.

The same flag can move a task's `pool`, `queue`, `executor`, or `priority_weight`, so infrastructure gets
canaried the way a feature does. That is the wedge: nothing else ramps a *subset of Airflow tasks* at the
scheduler level.

## How it's shaped

Two capabilities over one flag API, both no-ops until enabled:

- **Placement policy** (platform): an Airflow
  [cluster policy](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/cluster-policies.html)
  reads a flag at parse time and sets a task's placement for a chosen subset. No DAG edit.
- **In-task evaluation** (author): a hook, sensor, and `gate` read a flag for a stable entity inside a
  task; an exposure listener records which group each run landed in.

OpenFeature is the only integration point, so any backend with an OpenFeature provider (flagd, Flipt, GO
Feature Flag, GrowthBook, Unleash, LaunchDarkly, Statsig, or an in-house engine) works with no code
change here.

## Architecture invariants (do not break)

- **OpenFeature is the only integration point.** Core imports `openfeature-sdk` and nothing
  backend-specific. Backends are optional extras and thin adapters under `providers/`, each an
  `AbstractProvider` (`get_metadata` + `resolve_*` + `get_provider_hooks`).
- **Three surfaces, three entry points** (`pyproject.toml`): `apache_airflow_provider` → `provider_info`
  (hook/sensor/connection); `airflow.policy` → `policy` (placement); `airflow.plugins` →
  `OpenFeaturePlugin` (exposure listener).
- **Install is a no-op.** `enable_policy`, `enable_exposure_listener`, and any UI stay off until set in
  `[openfeature]` config. No side effects on import.
- **Third-party namespace.** The package is `openfeature_airflow`, never `airflow.providers.*`. The wheel
  must not ship an `airflow/` namespace; CI asserts this.
- **Well-known flag keys are a public contract**: `airflow.task.pool|queue|executor|priority_weight`. Add
  keys; don't rename.
- **Deterministic cohorts.** The bundled in-process rollout provider buckets with `hashlib.sha256`, not
  the salted builtin `hash()`, so a task's cohort is stable across processes, machines, and restarts.
- **Lazy imports in policy/listener code.** Import `airflow.*` and backend SDKs inside functions, so
  importing the policy module never triggers Airflow settings init.
- **Fork safety.** In-task evaluation runs in a task worker, and Airflow 3.x forks that worker from a
  multi-threaded supervisor. Prefer HTTP-per-call backends (OFREP / Flipt / GO Feature Flag) for that
  path and build clients per process with `providers/forksafe.py::ForkSafeProvider`. Known limit: Airflow
  3.x LocalExecutor deadlocks any forked PythonOperator at startup regardless of provider, so
  live-scheduled in-task evaluation is a 2.x or `airflow tasks test` story; the parse-time placement
  policy is unaffected.

## Layout

| Path | What |
|---|---|
| `src/openfeature_airflow/policy.py` | placement policy (`apply_placement`, `task_policy`) |
| `.../gate.py`, `hooks/`, `sensors/` | in-task evaluation surfaces |
| `.../listener.py` | exposure emission |
| `.../providers/` | backend adapters (fractional, growthbook, unleash, statsig, inhouse, forksafe) |
| `tests/` | unit tests; `tests/integration/` needs `RUN_INTEGRATION=1` |
| `system_tests/` | real-backend e2e drivers + `E2E.md` evidence |
| `docs/` | architecture, use cases, extending, measurement |

The companion repo `airflow-provider-openfeature-example` holds a runnable demo and the always-on hosted
instance (real Airflow 3.x + a Flipt backend, no login). Keep its cross-links and version claims honest
when this package changes.

## Commands

```bash
pip install -e ".[dev]"                      # editable install with test deps
pytest tests/ -q -m "not integration"        # unit tests (hermetic)
ruff check src tests                         # lint
python -m build && twine check dist/*        # package
RUN_INTEGRATION=1 pytest tests/integration   # real backends (needs docker + an Airflow runtime)
```

The coverage gate is 95% (`--cov-fail-under=95`). CI runs the matrix Python 3.10/3.11 × Airflow 2.11/3.3,
plus lint, build, CodeQL, Scorecard, and Sonar.

## Keep everything in sync

A change is not done until the code, its tests, the docs that describe it, and these agent instructions
all reflect it, in the same PR. This is a rule, not a nicety.

- **Tests.** Every non-trivial change lands with a test. Provider adapters get a unit test with a
  duck-typed or `importorskip`'d SDK. Behavior touching real task execution gets a `system_tests/` driver
  with raw evidence, not a stamp.
- **Docs.** A change to behavior, the flag contract, the entry points, the placement dimensions, or the
  examples updates the README (what-you-get, how-it-works, the flag list), `docs/`, and the example DAGs
  in the same commit. Prefer generating docs from real runs (the demo images and `E2E.md` are rendered,
  not hand-edited).
- **Agent instructions.** If a convention, invariant, command, or the layout changes, update this file.
  `CLAUDE.md` and `.github/copilot-instructions.md` are symlinks to it, so every coding agent (Copilot,
  Claude, Codex, Copilot CLI) reads one source. Don't let them drift.

A PR that changes a public contract but not its docs, tests, or this guide gets sent back.

## Releasing

Conventional-commit titles drive [release-please](https://github.com/googleapis/release-please): `feat:`
bumps the minor, `fix:` the patch; `docs:` / `chore:` / `refactor:` don't release. release-please keeps a
release PR open with the next version and changelog; merging it tags the release, which triggers the
signed publish (Sigstore + PyPI attestations). That release PR needs "Allow GitHub Actions to create and
approve pull requests" enabled (Settings → Actions → General → Workflow permissions).

## Adding a backend

Only when a backend has no OpenFeature provider, or you want a convenience wrapper. Implement
`AbstractProvider` under `providers/`, add an optional extra in `pyproject.toml`, and a unit test. See
[`docs/extending.md`](docs/extending.md).

## Privacy boundary (this repo is public)

No proprietary or employer-internal backend, service name, cluster name, ticket id, or account belongs
here. A proprietary engine is a **private** adapter that depends on this package and implements
`AbstractProvider`; `providers/inhouse.py` is the public template. Keep examples generic (`inhouse`,
`default_pool`, `dag_id`).

## Style

- Root `LICENSE` + `NOTICE` only; no per-file license headers.
- Comments explain why, not what. One line unless a real gotcha needs more.
- Conventional-commit titles; PR bodies are why + what changed + how tested. Sign off every commit
  (`git commit -s`, DCO); no `Co-authored-by`.
