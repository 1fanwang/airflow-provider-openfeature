"""Generic OpenFeature code-path gate for evaluating flags anywhere in Airflow code."""

from __future__ import annotations

from openfeature import api
from openfeature.evaluation_context import EvaluationContext


def _ctx(entity: str, attrs: dict) -> EvaluationContext:
    return EvaluationContext(targeting_key=entity, attributes=attrs or {})


def flag_enabled(flag_key: str, entity: str, default: bool = False, **attrs) -> bool:
    """True/False for a boolean flag, keyed on ``entity`` (deterministic per backend)."""
    return api.get_client().get_boolean_value(flag_key, default, _ctx(entity, attrs))


def variant(flag_key: str, entity: str, default: str, **attrs) -> str:
    """The string variant for a multi-variant flag, keyed on ``entity``."""
    return api.get_client().get_string_value(flag_key, default, _ctx(entity, attrs))


def number(flag_key: str, entity: str, default: int, **attrs) -> int:
    """The integer value for a numeric flag, keyed on ``entity``."""
    return api.get_client().get_integer_value(flag_key, default, _ctx(entity, attrs))

