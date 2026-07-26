"""A dependency-free, in-process OpenFeature provider with deterministic percentage rollouts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from openfeature.flag_evaluation import FlagResolutionDetails, Reason
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

_ANONYMOUS = "__anonymous__"


def _bucket(entity: str, flag_key: str) -> int:
    """Return a deterministic bucket in ``0..99`` for ``(entity, flag_key)``.

    Stable across processes and machines (uses ``hashlib`` rather than the salted builtin ``hash``),
    so the same entity always lands in the same group for a given flag.
    """
    digest = hashlib.sha256(f"{flag_key}:{entity}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % 100


def entity_of(evaluation_context) -> str:
    if evaluation_context is None:
        return _ANONYMOUS
    attributes = evaluation_context.attributes or {}
    return evaluation_context.targeting_key or attributes.get("entity") or attributes.get("id") or _ANONYMOUS


@dataclass
class BoolFlag:
    """A boolean flag enabled for ``rollout_pct`` percent of entities (0..100).

    ``layer`` overrides the bucketing salt: flags sharing a layer randomize together, flags in
    different layers (or with distinct keys) randomize independently. See Google's overlapping
    experiment layers (Tang et al., KDD 2010).
    """

    rollout_pct: int
    layer: str | None = None


@dataclass
class VariantFlag:
    """A multi-variant string flag. ``variants`` is ``[(value, weight)]`` with weights summing to 100.

    ``layer`` overrides the bucketing salt (see :class:`BoolFlag`).
    """

    variants: list[tuple[str, int]]
    layer: str | None = None


class FractionalProvider(AbstractProvider):
    """In-process OpenFeature provider doing deterministic percentage rollouts.

    Useful for zero-dependency canary/percentage rollouts and for testing without a running flag
    daemon. For richer targeting or a shared source of truth, point OpenFeature at a backend such as
    flagd, Unleash, or GrowthBook instead; the evaluation call sites do not change.

    :param bool_flags: mapping of flag key to :class:`BoolFlag` (percentage rollout).
    :param variant_flags: mapping of flag key to :class:`VariantFlag` (weighted variants).
    """

    def __init__(
        self,
        bool_flags: dict[str, BoolFlag] | None = None,
        variant_flags: dict[str, VariantFlag] | None = None,
    ) -> None:
        self._bool_flags = dict(bool_flags or {})
        self._variant_flags = dict(variant_flags or {})

    def get_metadata(self) -> Metadata:
        return Metadata(name="FractionalProvider")

    def get_provider_hooks(self) -> list:
        return []

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        flag = self._bool_flags.get(flag_key)
        if flag is None:
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)
        enabled = _bucket(entity_of(evaluation_context), flag.layer or flag_key) < flag.rollout_pct
        return FlagResolutionDetails(
            value=enabled, variant="on" if enabled else "off", reason=Reason.TARGETING_MATCH
        )

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        flag = self._variant_flags.get(flag_key)
        if flag is None:
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)
        bucket = _bucket(entity_of(evaluation_context), flag.layer or flag_key)
        cumulative = 0
        for value, weight in flag.variants:
            cumulative += weight
            if bucket < cumulative:
                return FlagResolutionDetails(value=value, variant=value, reason=Reason.TARGETING_MATCH)
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

