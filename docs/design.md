# Design and roadmap

## Summary

`airflow-provider-openfeature` brings progressive delivery to Apache Airflow. A cluster policy reads an
[OpenFeature](https://openfeature.dev) flag at scheduler time and places a subset of DAG runs on a
different pool, queue, or executor, so a platform team ramps an infrastructure change, measures it, and
reverts with a flag instead of a redeploy. This page records why the design looks the way it does —
grounded in how large platforms run experiments — and what is left to build.

## The problem: safe ramps stop at the request layer

Feature flags and online experimentation are solved for user-facing traffic. Every vendor
([LaunchDarkly](https://launchdarkly.com), [GrowthBook](https://www.growthbook.io),
[Unleash](https://www.getunleash.io), [Statsig](https://statsig.com), [Eppo](https://www.geteppo.com))
and every in-house platform (Meta, LinkedIn, Netflix, Uber, Airbnb) evaluates a flag per request, for a
user. The [OpenFeature spec](https://openfeature.dev/specification/) itself defines the targeting key as
the end-user.

A data platform has no equivalent. Moving a fleet of pipelines to the KubernetesExecutor, a new worker
pool, or a faster pod-creation path ([apache/airflow#68480](https://github.com/apache/airflow/pull/68480))
is all-or-nothing: change a config, redeploy, and it hits every DAG at once. The unit that needs ramping
is a **pipeline run**, and it is time-indexed, not a request from a user. No feature-flag tool addresses
this — see [prior art](#prior-art). Eppo's warehouse-native assignment is the closest, and it still has
no pipeline-run entity.

## What ships today

Status: **Shipped** = in `main` · **Planned** = roadmap.

| Area | As a platform team, I can… | Capability | Status |
|---|---|---|---|
| Placement | move a subset of DAGs to a different pool / queue / executor / priority by flag, no DAG edit | cluster policy + [`register_placement`](extending.md) | Shipped |
| In-DAG | gate a code path or A/B a model inside a task | hook, sensor, `flag_enabled` | Shipped |
| Exposure | record which group each run landed in | exposure listener | Shipped |
| Measure | record an outcome and read lift per backend | [`track_outcome`](measurement.md) | Shipped |
| Backends | use flagd, GrowthBook, Unleash, Statsig, LaunchDarkly, or an in-house engine | OpenFeature + adapters | Shipped |

Assignment is deterministic — `hash(flag_key + dag_id) % 100` — matching how
[PlanOut](https://github.com/facebookarchive/planout), Statsig, and Uber bucket. That is the one design
decision the research validates as-is.

## What the research says to build next

Each item is motivated by how platforms at scale run experiments, and by the statistics of
infrastructure metrics (which differ from user metrics — see the caveat below).

| Area | The problem | The design | Grounded in | Status |
|---|---|---|---|---|
| Orthogonal layers | two infra experiments on the same DAGs collide | one layer per resource dimension; `hash(layer + dag_id)` independent per layer | Google [Overlapping Experiment Infrastructure, KDD 2010](https://dl.acm.org/doi/10.1145/1835804.1835810); [PlanOut](https://arxiv.org/abs/1409.3174) namespaces | Planned |
| Switchback | 40 DAGs is too small for cross-sectional A/B, and treated DAGs contend with control DAGs for shared pool slots (a SUTVA break) | randomize by time window, not by run; the whole cluster is the unit | [Bojinov, Simchi-Levi, Zhao — switchback designs](https://doi.org/10.1287/mnsc.2022.4536); DoorDash Curie | Planned |
| Exposure anchor | a DAG whose flag is evaluated but whose task never runs should not count | log exposure at `on_task_instance_running`, not at parse | Spotify Config Applied; Airbnb ERF | Planned |
| Always-valid inference | watching a ramp continuously with a fixed-horizon t-test inflates false positives | mSPRT / confidence sequences; gate auto-rollback on the always-valid bound | [Johari, Pekelis, Walsh — Always Valid Inference](https://arxiv.org/abs/1512.04922) | Planned |
| Variance reduction | small N needs sensitivity | CUPED on the prior-weeks value of the same DAG's metric (ρ ≈ 0.7–0.9 for infra) | [Deng, Xu, Kohavi, Walker — CUPED, WSDM 2013](https://dl.acm.org/doi/10.1145/2433396.2433413) | Planned |
| Trust checks | an uneven policy rollout across schedulers silently skews the split | SRM (χ²) + guardrail checks before any readout | [Fabijan et al. — Diagnosing SRM, KDD 2019](https://doi.org/10.1145/3292500.3330695) | Planned |
| Measurement | close the loop without a separate pipeline | warehouse-native: Airflow's metadata DB already carries stable IDs and outcomes | Statsig / Eppo warehouse-native | Planned |

**Infra metrics are not user metrics, and one caveat runs through all of the above.** Task duration,
slot utilization, and failure rate are highly autocorrelated (which makes CUPED unusually powerful),
heavy-tailed (which breaks the Gaussian assumption behind mSPRT — use a log or quantile outcome and a
robust bootstrap), and have no novelty effect (so the short-term result is the long-term result). The
trust machinery is there for operational safety and rollback, not a publication-grade p-value: at N = 40
pipelines the honest signal is "safe to proceed?", not "significant at α = 0.05".

## Beyond Airflow

The seam is orchestrator-specific; the pattern is not. The same flag-driven placement generalizes:

- **[Temporal](https://temporal.io)** — a client interceptor rewrites a workflow's task queue, routing a
  subset of runs to a canary worker fleet with no workflow-code change. Proven end to end on a local
  Temporal server; a `temporal-provider-openfeature` package is planned.
- **[Flyte](https://flyte.org)** — `with_overrides` sets a task's resources / image / pod spec at
  registration, and server-side matchable attributes route executions. The graph is static and routing
  is cluster-side, so this is heavier and comes after Temporal.

A shared core — deterministic bucketing, the OpenFeature seam, the measurement bridge — keeps the three
from diverging.

## How it fits

This complements your stack; it replaces nothing. OpenFeature is the seam. Your flag backend keeps
storing flags, targeting, and analysis; Airflow keeps scheduling. The provider carries the decision to
the one place they do not reach: the scheduler, at placement time. The bundled `FractionalProvider` is a
no-backend default for the quickstart and tests; production points at your real backend.

## Prior art

- **Vendors and the standard** — [OpenFeature spec](https://openfeature.dev/specification/),
  [LaunchDarkly](https://launchdarkly.com), [GrowthBook](https://www.growthbook.io),
  [Unleash](https://www.getunleash.io), [Statsig](https://statsig.com), [Eppo](https://www.geteppo.com).
  All request-time; Eppo's warehouse-native assignment is the nearest analog.
- **Assignment and layers** — [PlanOut](https://github.com/facebookarchive/planout) and its paper
  [Designing and Deploying Online Field Experiments](https://arxiv.org/abs/1409.3174) (Bakshy et al.);
  Google [Overlapping Experiment Infrastructure](https://dl.acm.org/doi/10.1145/1835804.1835810).
- **Small-N and time-indexed designs** — [switchback experiments](https://doi.org/10.1287/mnsc.2022.4536)
  (Bojinov et al.), DoorDash Curie; network interference
  [Ugander et al., KDD 2013](https://dl.acm.org/doi/10.1145/2487575.2487620).
- **Inference and trust** — [Trustworthy Online Controlled Experiments](https://experimentguide.com/)
  (Kohavi, Tang, Xu); [CUPED](https://dl.acm.org/doi/10.1145/2433396.2433413);
  [Always Valid Inference](https://arxiv.org/abs/1512.04922);
  [Diagnosing SRM](https://doi.org/10.1145/3292500.3330695).
- **Release engineering** — [feature-toggle taxonomy](https://martinfowler.com/articles/feature-toggles.html),
  [Google SRE canarying](https://sre.google/workbook/canarying-releases/),
  [Argo Rollouts analysis](https://argoproj.github.io/argo-rollouts/features/analysis/).
