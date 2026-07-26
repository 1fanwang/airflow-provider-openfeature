# Changelog

Notable changes to this project. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - unreleased (alpha)

### Added
- OpenFeature-backed cluster policy for flag-driven `pool` / `queue` / `executor` / `priority_weight`
  placement, keyed on `dag_id:task_id` and gated behind `[openfeature] enable_policy`.
- `register_placement(flag_key, setter, kind=...)` to add custom flag-driven placement dimensions for any
  operator attribute, applied in the same policy pass as the built-ins.
- Author-facing `OpenFeatureHook`, `FeatureFlagSensor`, and a code-path `gate` for evaluating flags
  inside DAGs.
- Exposure listener that records the resolved group/variant for measurement.
- `measure.track_outcome` for recording an experiment outcome through the OpenFeature tracking API,
  with native `track()` bridges on the Statsig, GrowthBook, and in-house adapters plus a tagged
  StatsD/OTEL metric for backends without analytics.
- Backend adapters: flagd, GrowthBook, Unleash, Statsig, an in-house template, and a dependency-free
  fractional provider.
- Runnable example DAGs (revenue-rollup canary, 2→3 migration, KubernetesExecutor canary, A/B a model)
  and two worked case studies with real backends.
- Entry points auto-registering the provider, policy, and plugin, so `pip install` wires all three
  with no `airflow_local_settings` edits.
- Tested against Apache Airflow 2.11 and 3.3 on Python 3.10 and 3.11.

### Fixed
- The placement policy no longer raises `AirflowClusterPolicyError` when a task attribute is read-only
  (a mapped operator's `executor`), which previously broke DAG parsing for the whole file when the
  executor flag was set. Each placement is now skipped if the operator does not accept it.
