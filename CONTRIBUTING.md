# Contributing

Thanks for your interest. This is a third-party Apache Airflow provider; it is not in the Airflow
monorepo, and it aims to be listed on the Airflow Ecosystem page.

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

`.[dev]` pulls in the test backends (GrowthBook, Statsig, Unleash) so the provider tests run.

## Before you open a PR

```bash
pytest tests/ -q -m "not integration"    # unit tests, hermetic
ruff check src tests                      # lint
python -m build && twine check dist/*     # package builds and is well-formed
```

All three must be green. The build job also asserts the wheel ships no `airflow/` namespace, a
third-party provider must live under its own top-level package.

Integration tests hit real backends and need Docker plus an Airflow runtime:

```bash
RUN_INTEGRATION=1 pytest tests/integration -m integration
```

## What a good change looks like

- Read [AGENTS.md](AGENTS.md) first, it lists the architecture invariants (OpenFeature is the only
  integration point, install is a no-op, third-party namespace, well-known flag keys).
- Every non-trivial change lands with a test. New backend adapters get a unit test with a duck-typed
  or `importorskip`'d SDK; behavior that affects real task execution gets a `system_tests/` driver
  with raw evidence, not a stamp.
- Keep the PR body to why, what changed, and how you tested it.

## Adding a backend

See [docs/extending.md](docs/extending.md). In short: implement OpenFeature's `AbstractProvider` under
`src/openfeature_airflow/providers/`, add an optional extra in `pyproject.toml`, and a unit test. If
the backend already ships its own OpenFeature provider, you may not need an adapter here at all.

## Branch protection (repo maintainers)

The `main` branch must be protected so that unfinished or failing CI blocks merges. In
**Settings → Branches → main → Edit rule**:

1. Enable **Require status checks to pass before merging**.
2. Enable **Require branches to be up to date before merging**.
3. Add `ci-success` as the only required check (the CI workflow has a gate job with that name
   that requires the full test matrix, lint, and build to all pass).

With this one check in place, a PR whose CI has not run yet, is still running, was cancelled,
or has any failing job will be blocked from merging.

## Licensing

By contributing you agree your work is licensed under Apache-2.0. The repo uses a root `LICENSE` and
`NOTICE`; there are no per-file headers.
