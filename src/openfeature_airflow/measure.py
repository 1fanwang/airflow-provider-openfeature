"""The measure half of assign -> expose -> measure -> decide.

``track_outcome`` records an experiment outcome (task duration, success, cost) for an entity, tagged
with its group. It routes through the OpenFeature tracking API so any backend with a native event
API receives it (Statsig ``log_event``, LaunchDarkly ``track``, a GrowthBook/PostHog callback), and
increments a tagged ``openfeature.outcome.<metric>`` StatsD/OTEL metric so backends without analytics
(flagd) still land the signal in your warehouse. Safe no-op if the provider has no tracking.
"""

from __future__ import annotations

from openfeature import api
from openfeature.evaluation_context import EvaluationContext


def track_outcome(metric: str, entity: str, value: float = 1.0, **attrs) -> None:
    """Emit an outcome for ``entity`` (keyed like the gate), tagged with group attributes."""
    clean = {k: v for k, v in attrs.items() if v is not None}
    ctx = EvaluationContext(targeting_key=entity, attributes=clean)
    try:
        from openfeature.track import TrackingEventDetails

        api.get_client().track(metric, ctx, TrackingEventDetails(value=value, attributes=clean or None))
    except Exception:  # provider may not implement tracking; the metric below still lands
        pass
    _emit_metric(metric, value, clean)


def _emit_metric(metric: str, value: float, tags: dict) -> None:
    try:
        try:
            from airflow.sdk.observability.stats import Stats  # Airflow 3.x
        except ImportError:
            from airflow.stats import Stats  # Airflow 2.x

        str_tags = {k: str(v) for k, v in tags.items()}
        Stats.incr(f"openfeature.outcome.{metric}", tags=str_tags)
        if value is not None:
            Stats.gauge(f"openfeature.outcome.{metric}.value", float(value), tags=str_tags)
    except Exception:  # metrics are best-effort
        pass
