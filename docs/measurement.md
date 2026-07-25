# Measuring an experiment (the readout side)

Placement and gating decide **who runs what**. To run a real experiment you also need the outcome:
**assign → run → measure → decide**. This package does the measure step with one call.

```python
from openfeature_airflow.measure import track_outcome

# in your task, after the work is done:
track_outcome("task_duration_ms", f"{dag_id}:{task_id}", value=elapsed_ms,
              variant=cohort, dag_id=dag_id)
```

`track_outcome` routes through the OpenFeature tracking API, so any backend that implements `track()`
receives the outcome, and it also increments a tagged `openfeature.outcome.<metric>` StatsD/OTEL
metric so backends without analytics still land the signal in your warehouse.

## Where the outcome shows up, per backend

| Backend | How the outcome is read out |
|---|---|
| flagd / OFREP | no native analytics; the `openfeature.outcome.*` metric goes to StatsD/OTEL → Grafana/Prometheus |
| Statsig | the bundled adapter's `track()` calls `log_event`; shows in Pulse/metrics |
| GrowthBook | pass `on_track=` to the adapter; it hands the outcome to your warehouse (GrowthBook measures there) |
| LaunchDarkly | `track()` → LD `track` events feed LD Experimentation (recipe below) |
| in-house | the template adapter appends to a `tracked` export list |

Proven end to end for flagd, Statsig, and the in-house engine in
[`system_tests/measure_loop.py`](../system_tests/measure_loop.py) — real backends, real measured lift.

## The warehouse path (works with any backend)

Turn on the metric and read the cohort-tagged outcome in Grafana or Prometheus:

```ini
[metrics]
statsd_on = True
statsd_datadog_enabled = True     # emits the variant/dag_id tags
```

`openfeature.outcome.task_duration_ms.value` carries `variant` and `dag_id` tags, so control vs
treatment is a `group by variant` in your dashboard. This is the whole readout for flagd/OFREP, and a
backstop for the platforms.

## GrowthBook (warehouse-native)

```python
from openfeature_airflow.providers.growthbook import GrowthBookProvider

def to_warehouse(metric, entity, value, attrs):
    ...  # INSERT into your events table; GrowthBook computes lift from it

api.set_provider(GrowthBookProvider(features=..., on_track=to_warehouse))
```

## LaunchDarkly

Use LaunchDarkly's own OpenFeature provider for assignment. For the outcome, call LD `track` in your
measure step (their provider does not forward OpenFeature `track()` yet):

```python
import ldclient

ldclient.get().track("task_duration_ms", ld_context, metric_value=elapsed_ms)
```

## PostHog

Use PostHog's provider for assignment; emit the outcome with `capture`:

```python
posthog.capture(distinct_id=entity, event="task_duration_ms",
                properties={"variant": cohort, "value": elapsed_ms})
```

## Which metric to measure

The honest outcome for a data pipeline is usually one of: task duration, success/failure rate, SLA
hit rate, rows processed, or cost. Measure it from the task itself (as above) or from Airflow's own
task metrics, tagged by the cohort the exposure listener recorded.
