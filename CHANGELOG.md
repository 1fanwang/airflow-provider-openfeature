# Changelog

Notable changes to this project. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2](https://github.com/1fanwang/airflow-provider-openfeature/compare/airflow-provider-openfeature-v0.2.1...airflow-provider-openfeature-v0.2.2) (2026-07-27)


### Bug Fixes

* make release asset-attach best-effort for immutable releases ([#26](https://github.com/1fanwang/airflow-provider-openfeature/issues/26)) ([fea58bd](https://github.com/1fanwang/airflow-provider-openfeature/commit/fea58bd8fbd8f331ec669ff4f0ef6ffb837e467e))
* publish releases from the release-please run ([#23](https://github.com/1fanwang/airflow-provider-openfeature/issues/23)) ([7dfed06](https://github.com/1fanwang/airflow-provider-openfeature/commit/7dfed06dc5fec333f1f86a58f7e627b1bf16e023))

## [0.2.1](https://github.com/1fanwang/airflow-provider-openfeature/compare/airflow-provider-openfeature-v0.2.0...airflow-provider-openfeature-v0.2.1) (2026-07-27)


### Bug Fixes

* publish on release-please's component-prefixed tags ([#21](https://github.com/1fanwang/airflow-provider-openfeature/issues/21)) ([fb784a7](https://github.com/1fanwang/airflow-provider-openfeature/commit/fb784a7cd58bd1046b66b587fcefba620693b08a))


### Documentation

* add 'When to reach for this' and guard the determinism claim ([#20](https://github.com/1fanwang/airflow-provider-openfeature/issues/20)) ([4314c98](https://github.com/1fanwang/airflow-provider-openfeature/commit/4314c98c4f26e750125c8c3badf41b2e364dd737))
* drop a personal git preference from the public agent guide ([#22](https://github.com/1fanwang/airflow-provider-openfeature/issues/22)) ([6e37809](https://github.com/1fanwang/airflow-provider-openfeature/commit/6e37809adcc6970b913538e203bed2765b9042c1))
* final prose pass (em-dashes, header) ([#18](https://github.com/1fanwang/airflow-provider-openfeature/issues/18)) ([25a77b2](https://github.com/1fanwang/airflow-provider-openfeature/commit/25a77b24bfa45f6d05fbcb8551c1e5fe2469281e))

## [0.2.0](https://github.com/1fanwang/airflow-provider-openfeature/compare/airflow-provider-openfeature-v0.1.0...airflow-provider-openfeature-v0.2.0) (2026-07-27)


### Features

* add ForkSafeProvider for per-process, fork-safe client init ([#13](https://github.com/1fanwang/airflow-provider-openfeature/issues/13)) ([ad06e1b](https://github.com/1fanwang/airflow-provider-openfeature/commit/ad06e1b9d69dd05361884a77512b4a4e8e2a6e1d))


### Documentation

* front-load a Quickstart, surface demo + docs, add term links ([#17](https://github.com/1fanwang/airflow-provider-openfeature/issues/17)) ([5a8b630](https://github.com/1fanwang/airflow-provider-openfeature/commit/5a8b6302681a14e41c2f2fc82f1b9810328467ba))
* lead onboarding with real Airflow and route by persona ([#1](https://github.com/1fanwang/airflow-provider-openfeature/issues/1)) ([fc6a2e3](https://github.com/1fanwang/airflow-provider-openfeature/commit/fc6a2e3dcfe50c1183d8fe3c2e006bdc06de9cad))
* point the live-demo references at Flipt ([#12](https://github.com/1fanwang/airflow-provider-openfeature/issues/12)) ([2c97938](https://github.com/1fanwang/airflow-provider-openfeature/commit/2c9793843ecb8c493d3614b6d5d2fc0452bdd53f))
* README list + DeepWiki badge; drop CodeRabbit config ([#2](https://github.com/1fanwang/airflow-provider-openfeature/issues/2)) ([6818651](https://github.com/1fanwang/airflow-provider-openfeature/commit/68186518e19abb065d4854c20d649cfa4fabf44f))
* surface the live demo as the zero-install way in ([#11](https://github.com/1fanwang/airflow-provider-openfeature/issues/11)) ([7aa2999](https://github.com/1fanwang/airflow-provider-openfeature/commit/7aa2999e5cb4d0b19304d4d6a0cee8d37a3e4f6f))
* vision-first AGENTS.md, shared with every agent via symlinks ([#16](https://github.com/1fanwang/airflow-provider-openfeature/issues/16)) ([f839848](https://github.com/1fanwang/airflow-provider-openfeature/commit/f8398486b9cf64333fc287ecfe4b9ad102bc2e20))

## [Unreleased]

### Added
- `openfeature_airflow.switchback`: time-windowed (switchback) assignment for small pipeline
  populations that share pools and executors, assigning whole time windows to treatment or control.
- `openfeature_airflow.analysis`: a sample-ratio-mismatch check (`srm_check`, chi-square, standard
  library only) and a `lift` helper, to validate a ramp's split before reading any outcome.
- `layer` on `BoolFlag` / `VariantFlag` to group or namespace the FractionalProvider bucketing salt for
  orthogonal experiments.

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
