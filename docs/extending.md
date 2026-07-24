# Extending this, and engaging the wider ecosystem

The short version: **integrate the standard once, not each vendor.** OpenFeature is already the
pluggable layer. This package targets the OpenFeature provider and evaluation API, so any backend
that ships an OpenFeature provider works here with no code in this repo, flagd, GrowthBook, Unleash,
Flagsmith, LaunchDarkly, ConfigCat, Split, and others. That is the point: many communities, one
seam.

So "deep integration with everything" is a trap when read as N bilateral integrations. Depth belongs
in two places only:

1. **The standard (OpenFeature).** Contribute exposure hooks, examples, and an ecosystem listing.
2. **The Airflow seam.** The policy, the listener, and the hook/sensor, the part OpenFeature does
   not have, and what makes flags mean something at the platform level.

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

## Engaging each community

| Community | What to contribute |
|---|---|
| OpenFeature (CNCF) | The anchor. Get listed on their ecosystem page; contribute the Airflow exposure hook; show Airflow as a consumer in their docs. |
| Airflow (ASF) | Register on the [Ecosystem page](./ecosystem-entry.md) now. A dev-list `[DISCUSS]` for an in-tree provider comes later, per `ACCEPTING_PROVIDERS`, only with traction and a sponsor. |
| flagd | The reference backend, same CNCF family. Use it as the canonical demo; contribute the Airflow example. |
| GrowthBook, Unleash, Flagsmith | Each ships an OpenFeature provider. Upstream the fixes you hit while wiring them, plus a short "using X with Airflow" doc. This is the highest-signal contribution: a fix in the provider helps every consumer, Airflow included. |
| Commercial (LaunchDarkly, Statsig, ...) | Use their own OpenFeature provider, or a thin adapter. Do not over-invest. |

The strongest engagement is upstreaming real fixes to the provider repos you exercise, not writing
more adapters here. Writing to the standard is what earns the integration.

## Rules that keep it extensible

- Never hardcode a backend. The core imports only `openfeature-sdk`.
- Adapters stay thin and sit behind the OpenFeature interface.
- The policy reads documented well-known flag keys (`airflow.task.pool`, `airflow.task.queue`,
  `airflow.task.executor`). Add keys without breaking existing ones.
- Config-gated no-ops plus entry-point discovery: installing changes nothing until a flag turns it
  on.

## The tension, resolved

You cannot be bilaterally deep with every vendor and stay pluggable. Put the depth in the standard
and the Airflow seam, let per-vendor detail pass through the evaluation context, and you get both.
One seam, many plugs.
