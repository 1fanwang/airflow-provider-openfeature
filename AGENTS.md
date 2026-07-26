# AGENTS.md

Agent + contributor guide for `airflow-provider-openfeature`. Read this before changing code.

## What this is

A third-party Apache Airflow provider that wires [OpenFeature](https://openfeature.dev) into Airflow.
Two capabilities over one flag API: evaluate flags inside a DAG (hook / sensor / gate), and gate the
platform by subset (a cluster policy places `pool` / `queue` / `executor`). OpenFeature is the abstraction layer,
so any backend that ships an OpenFeature provider works without code here.

## Architecture invariants (do not break these)

- **OpenFeature is the only integration point.** Core imports `openfeature-sdk` and nothing
  backend-specific. Backends are optional extras and thin adapters under `providers/`.
- **Three surfaces, three entry points** (see `pyproject.toml`):
  `apache_airflow_provider` → `provider_info` (hook/sensor/connection);
  `airflow.policy` → `openfeature_airflow.policy` (placement);
  `airflow.plugins` → `OpenFeaturePlugin` (exposure listener).
- **Install is a no-op.** The policy and listener stay off until `[openfeature] enable_policy` /
  `enable_exposure_listener` are set. Keep it that way, no side effects on import.
- **Third-party namespace.** The package is `openfeature_airflow`, never `airflow.providers.*`. The
  wheel must not ship an `airflow/` namespace; CI asserts this. Do not add `airflow/__init__.py`.
- **Well-known flag keys are a public contract**: `airflow.task.pool`, `airflow.task.queue`,
  `airflow.task.executor`, `airflow.task.priority_weight`. Add keys; don't rename existing ones.
- **Lazy imports in policy/listener code.** Import `airflow.*` and backend SDKs inside functions, not
  at module top level, so importing the policy module never triggers Airflow settings init.

## Layout

| Path | What |
|---|---|
| `src/openfeature_airflow/policy.py` | placement policy (`apply_placement`, `task_policy` hookimpl) |
| `src/openfeature_airflow/gate.py` | code-path eval (`flag_enabled`, `variant`) |
| `src/openfeature_airflow/listener.py` | exposure emission |
| `src/openfeature_airflow/hooks/`, `sensors/` | author-facing hook + sensor |
| `src/openfeature_airflow/providers/` | backend adapters (fractional, growthbook, unleash, statsig, inhouse) |
| `tests/` | unit tests; `tests/integration/` needs `RUN_INTEGRATION=1` |
| `system_tests/` | real-backend e2e drivers + `E2E.md` evidence |
| `docs/` | architecture, use cases, extending, ecosystem |

## Commands

```bash
pip install -e ".[dev]"                      # editable install with test deps
pytest tests/ -q -m "not integration"        # unit tests (hermetic)
ruff check src tests                         # lint
python -m build && twine check dist/*        # package
RUN_INTEGRATION=1 pytest tests/integration   # real backends (needs docker + an Airflow runtime)
```

Every non-trivial change lands with a test. Provider adapters get a unit test with a duck-typed or
`importorskip`'d SDK. Behavior that touches real task execution gets a `system_tests/` driver with raw
evidence in `E2E.md`, not a stamp.

## Code and docs move together

A change to behavior, the flag contract, the entry points, the placement dimensions, or the examples is
not complete until the docs that describe it change in the same commit. That means the README
(What-you-get, How-it-works, the well-known flag list), `docs/architecture.md`, `docs/use-cases.md`,
`docs/measurement.md`, the example DAGs, and this file. A PR that changes a public contract but not its
docs will be sent back.

Prefer generating docs from code over hand-writing them, so they cannot drift:

- The demo images (`docs/demo*.svg`, `docs/case-study/img/*`) are rendered from real runs by
  `system_tests/make_demo_svgs.py` and `make_demo_gif.py`. Regenerate them; do not hand-edit an image or
  paste numbers a run did not produce.
- `system_tests/E2E.md` holds captured real output, not prose claims. Update it by re-running the driver.
- The example DAGs are executed and checked by `system_tests/verify_examples_e2e.py`; if you change an
  example or the behavior it shows, re-run it and keep its output truthful.
- If you change the well-known flag keys or the placement dimensions in `policy.py`, update the flag
  list in the README and `docs/architecture.md` to match in the same change.

## Adding a backend

Only when a backend has no OpenFeature provider, or you want a convenience wrapper. Implement
OpenFeature's `AbstractProvider` under `providers/`, add an optional extra in `pyproject.toml`, and a
unit test. See [`docs/extending.md`](docs/extending.md).

## Privacy boundary (this repo is public)

- No proprietary or employer-internal backend, service name, cluster name, ticket id, or account
  belongs here. A proprietary engine is a **private** adapter that depends on this package and
  implements `AbstractProvider` like any other; `providers/inhouse.py` is the public template.
- Keep examples generic (`inhouse`, `default_pool`, `dag_id`). No company-specific identifiers in
  code, tests, docs, commits, or PR text.

## Style

- Root `LICENSE` + `NOTICE` only; no per-file license headers (matches Cosmos and other third-party
  providers).
- Comments explain why, not what. One line unless a real gotcha needs more.
- Conventional-commit-ish messages; keep PR bodies to why + what changed + how tested.
