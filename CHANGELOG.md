# Changelog

Notable changes to this project. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - unreleased (alpha)

### Added
- OpenFeature-backed cluster policy for flag-driven `pool` / `queue` / `executor` placement, keyed on
  `dag_id:task_id` and gated behind `[openfeature] enable_policy`.
- Author-facing `OpenFeatureHook`, `FeatureFlagSensor`, and a code-path `gate` for evaluating flags
  inside DAGs.
- Exposure listener that records the resolved cohort/variant for measurement.
- Backend adapters: flagd, GrowthBook, Unleash, Statsig, an in-house template, and a dependency-free
  fractional provider.
- Entry points auto-registering the provider, policy, and plugin, so `pip install` wires all three
  with no `airflow_local_settings` edits.
- Tested against Apache Airflow 2.11 and 3.3 on Python 3.10 and 3.11.
