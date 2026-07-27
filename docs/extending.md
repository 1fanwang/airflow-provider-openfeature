# Extending this

The short version: **integrate the standard once, not each vendor.** OpenFeature is already the
pluggable layer. This package targets the OpenFeature provider and evaluation API, so any backend with
an OpenFeature provider works here with no code in this repo: flagd, GrowthBook, Unleash, Flagsmith,
LaunchDarkly, ConfigCat, Split. One integration, every backend.

Depth belongs in two places:

1. **The standard (OpenFeature).** Contribute exposure hooks, examples, and an ecosystem listing.
2. **The Airflow side.** The policy, the listener, and the hook/sensor. This is what makes a flag mean
   something at the platform level, and OpenFeature does not provide it.

Per-vendor specifics (GrowthBook stickiness, LaunchDarkly contexts, a custom bucketing salt) ride
through OpenFeature's evaluation context. They do not get special-cased in this code.

## Two extension axes

**Backend axis.** `api.set_provider(anything)`. The bundled adapters (`growthbook`, `unleash`,
`statsig`, `inhouse`, `fractional`) are conveniences and worked examples; the real contract is
OpenFeature's `AbstractProvider`. A new backend is a `set_provider` call, not a PR to this repo.

**Surface axis.** Three entry points, `apache_airflow_provider` (hook/sensor/connection),
`airflow.policy` (placement), `airflow.plugins` (exposure listener). A new Airflow decision point
registers as another entry point; the core does not change.

The core depends only on `openfeature-sdk`. Backends are optional extras. That is what keeps it
flexible.

## Adding a backend adapter

Only needed when a backend has no OpenFeature provider yet, or you want a thin convenience wrapper.
Implement the OpenFeature interface and drop it under `providers/`:

```python
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata
from openfeature.flag_evaluation import FlagResolutionDetails, Reason

class MyBackendProvider(AbstractProvider):
    def get_metadata(self): return Metadata(name="MyBackendProvider")
    def get_provider_hooks(self): return []
    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        value = my_sdk.evaluate(flag_key, entity=evaluation_context.targeting_key)
        return FlagResolutionDetails(value=value, variant=value, reason=Reason.TARGETING_MATCH)
    # boolean / integer / float / object variants likewise
```

A **proprietary or in-house engine** follows the same shape. Keep that adapter in a private repo that
depends on this package; it implements `AbstractProvider` like any other backend and never lands
here. `providers/inhouse.py` is the public template for that pattern.

## Custom placement dimensions

The policy ships four flag-driven dimensions (`pool`, `queue`, `executor`, `priority_weight`). If your
operators carry other attributes you want to ramp behind a flag, like a Spark version, a checkpoint
toggle, or a routing key, you can register your own dimension. `register_placement(flag_key, setter, kind=...)`
adds it to the same policy pass; `setter(task, value)` runs when the flag resolves for a task's subset:

```python
# in airflow_local_settings.py or a bootstrap
from openfeature_airflow.policy import register_placement

# a string dimension: set an operator attribute from the flag's value
register_placement("airflow.task.spark_version", lambda t, v: setattr(t, "spark_version", v))

# a boolean, expressed as a "true"/"false" variant the setter coerces
register_placement("airflow.task.enable_checkpoint",
                   lambda t, v: setattr(t, "enable_checkpoint", v == "true"))

# a number dimension reads an integer flag
register_placement("airflow.task.retries", lambda t, v: setattr(t, "retries", v), kind="number")
```

A setter that raises is skipped, so a bad dimension never breaks DAG parsing. This turns a change that
used to be an in-operator `if flag_enabled(...)` into a policy the platform team ramps centrally, with
no DAG edits.

## Fork safety (Airflow 3.x)

The placement policy evaluates flags at parse time in the dag-processor, so it is unaffected by task
forking. In-task evaluation (the hook, sensor, and `gate`) runs inside a task worker, and Airflow 3.x's
LocalExecutor forks that worker from a multi-threaded Task SDK supervisor. A backend client that opens a
connection pool or a polling thread in `__init__` can be inherited across that fork and deadlock the
child.

Two things help:

- Prefer an HTTP-per-call backend (OFREP, Flipt, GO Feature Flag, GrowthBook) for the in-task path.
- Build the client per process, after the fork, with `ForkSafeProvider`:

```python
from openfeature import api
from openfeature.contrib.provider.flipt import FliptProvider
from openfeature_airflow.providers.forksafe import ForkSafeProvider

api.set_provider(ForkSafeProvider(lambda: FliptProvider(base_url="http://flipt:8080")))
```

The wrapped provider is built on the first evaluation, which happens in the child. This removes the
provider's client as a source of fork deadlocks. It does not make forking safe on its own: if the parent
is multi-threaded for reasons outside the provider, the child can still deadlock during Airflow's own
worker startup, before any flag code runs.

## Rules that keep it extensible

- Never hardcode a backend. The core imports only `openfeature-sdk`.
- Adapters stay thin and sit behind the OpenFeature interface.
- The policy reads documented well-known flag keys (`airflow.task.pool`, `airflow.task.queue`,
  `airflow.task.executor`, `airflow.task.priority_weight`). Add keys without breaking existing ones.
- Config-gated no-ops plus entry-point discovery: installing changes nothing until a flag turns it
  on.

## Deep and pluggable at once

You can't be deep with every vendor and stay pluggable. Put the depth in the standard and the Airflow
side, and let per-vendor detail pass through the evaluation context.
