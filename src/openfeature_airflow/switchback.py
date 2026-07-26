"""Switchback assignment: randomize by time window, not by run.

A small pipeline population that shares pools and executors is a poor fit for cross-sectional A/B: it
is underpowered, and treated and untreated DAGs interfere through the shared resource. Switchback
assigns whole time windows to treatment or control, so the cluster is the unit and the comparison is
across time. Use the returned time-bucket string as the OpenFeature targeting key. See Bojinov,
Simchi-Levi, Zhao, "Design and Analysis of Switchback Experiments" (Management Science, 2023).
"""

from __future__ import annotations

import time as _time

from openfeature.evaluation_context import EvaluationContext

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def window_seconds(window: str) -> int:
    """Parse a window like ``'30m'``, ``'1h'``, ``'2d'`` into seconds."""
    window = window.strip()
    unit = window[-1:]
    if unit not in _UNITS or not window[:-1].isdigit():
        raise ValueError(f"window must be <int><unit> with unit in {sorted(_UNITS)}, got {window!r}")
    return int(window[:-1]) * _UNITS[unit]


def time_bucket(window: str = "1h", now: float | None = None) -> str:
    """The current switchback window id: ``floor(now / window)``.

    Stable within a window and changes across windows, so an entity keyed on it flips treatment only
    at window boundaries.
    """
    secs = window_seconds(window)
    epoch = _time.time() if now is None else now
    return f"{window}#{int(epoch // secs)}"


def switchback_context(window: str = "1h", now: float | None = None, entity: str | None = None,
                       **attrs) -> EvaluationContext:
    """An OpenFeature context keyed on the time window (whole-cluster switchback).

    Pass ``entity`` to scope the switchback per DAG (``entity:bucket``) rather than cluster-wide.
    Extra keyword attributes are attached to the context for targeting.
    """
    bucket = time_bucket(window, now)
    key = f"{entity}:{bucket}" if entity else bucket
    return EvaluationContext(targeting_key=key, attributes={"time_bucket": bucket, **attrs})
