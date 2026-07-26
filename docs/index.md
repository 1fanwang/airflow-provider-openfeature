# airflow-provider-openfeature

**Feature flags for Apache Airflow.** Ramp a change across your DAGs, measure it, and revert with a
flag, not a redeploy.

Feature flags let you control how your Airflow platform behaves at runtime, across many DAGs, without
editing them or redeploying. A platform team can move a subset of tasks to a different pool, queue, or
executor, ramp a worker or executor migration, or flip a kill switch during an incident, centrally and
without touching anyone's DAG. A DAG author can gate a code path or A/B a model inside a task. Both go
through [OpenFeature](https://openfeature.dev), so it works with the flag backend you already run:
[flagd](https://flagd.dev), [LaunchDarkly](https://launchdarkly.com), [GrowthBook](https://www.growthbook.io),
[Unleash](https://www.getunleash.io), [Statsig](https://statsig.com), or an in-house engine.

## Install

```bash
pip install airflow-provider-openfeature
```

It is a standard PyPI package, so [uv](https://docs.astral.sh/uv/) works the same way:
`uv pip install airflow-provider-openfeature`.

## Pick your path

**I run the Airflow platform.** Install the provider, enable the policy, and move a subset of DAGs to a
different pool, queue, or executor from a flag, without touching any DAG.
→ [Getting started](getting-started.md), about 5 minutes on a local Airflow.

**I write DAGs.** Gate a code path or A/B a model inside a task with one call, no platform access needed.
→ [Use cases](use-cases.md) and the [example DAGs](https://github.com/1fanwang/airflow-provider-openfeature/tree/main/example_dags).

## More

- [Running a rollout](running-a-rollout.md): the day-to-day ramp, measure, and revert loop.
- [Case study](case-study/README.md): canary a faster pipeline step end to end, with a real backend.
- [Measurement](measurement.md): close the loop with your experiment platform or warehouse.
- [Architecture](architecture.md): the flow and the surfaces it registers.
- [Extending](extending.md): add a backend.

The [README on GitHub](https://github.com/1fanwang/airflow-provider-openfeature) has the demos and the full picture.
