"""Trust checks for a rollout: sample-ratio mismatch and lift, standard library only.

Sample-ratio mismatch (SRM) is the first check to run before reading any outcome. If the observed
split across groups does not match the split you configured, the assignment is broken -- a flag-rule
bug, or a policy deployed unevenly across schedulers -- and no outcome number can be trusted. See
Fabijan et al., "Diagnosing Sample Ratio Mismatch in Online Controlled Experiments" (KDD 2019).
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class SRMResult:
    chi_square: float
    p_value: float
    ok: bool  # False when p < threshold, i.e. the observed split does not match the expected one
    observed: dict[str, int]
    expected: dict[str, float]


def srm_check(observed: dict[str, int], expected: dict[str, float] | None = None,
              threshold: float = 0.001) -> SRMResult:
    """Chi-square goodness-of-fit test that observed group counts match the expected split.

    :param observed: group -> count actually seen, e.g. ``{"canary": 48, "control": 452}``.
    :param expected: group -> expected proportion or weight (keys must match ``observed``); defaults to
        an equal split. Weights are normalized, so ``{"canary": 10, "control": 90}`` means 10/90.
    :param threshold: p-value below which the split is flagged. 0.001 is the common bar.
    :returns: an :class:`SRMResult`; ``ok`` is False when ``p < threshold``.
    """
    groups = list(observed)
    n = sum(observed.values())
    if n == 0 or len(groups) < 2:
        return SRMResult(0.0, 1.0, True, dict(observed), {})
    weights = {g: 1.0 for g in groups} if expected is None else expected
    total_w = sum(weights[g] for g in groups)
    exp_counts = {g: n * weights[g] / total_w for g in groups}
    chi2 = sum((observed[g] - exp_counts[g]) ** 2 / exp_counts[g] for g in groups)
    p = _chi2_sf(chi2, df=len(groups) - 1)
    return SRMResult(chi2, p, p >= threshold, dict(observed), exp_counts)


def lift(control_mean: float, treatment_mean: float) -> float | None:
    """Relative change of treatment over control (0.12 == +12%). None when control is 0."""
    if control_mean == 0:
        return None
    return (treatment_mean - control_mean) / control_mean


# --- chi-square survival function, standard library only (regularized incomplete gamma) ----------
def _chi2_sf(x: float, df: int) -> float:
    """P(chi-square with ``df`` degrees of freedom > ``x``), computed as Q(df/2, x/2)."""
    if x <= 0:
        return 1.0
    return _gammaq(df / 2.0, x / 2.0)


def _gammaq(s: float, x: float) -> float:
    """Regularized upper incomplete gamma Q(s, x) = 1 - P(s, x)."""
    if x < s + 1.0:
        return 1.0 - _gammap_series(s, x)
    return _gammaq_cf(s, x)


def _gammap_series(s: float, x: float, iters: int = 300, eps: float = 1e-14) -> float:
    ap, term, total = s, 1.0 / s, 1.0 / s
    for _ in range(iters):
        ap += 1.0
        term *= x / ap
        total += term
        if abs(term) < abs(total) * eps:
            break
    return total * math.exp(-x + s * math.log(x) - math.lgamma(s))


def _gammaq_cf(s: float, x: float, iters: int = 300, eps: float = 1e-14) -> float:
    tiny = 1e-300
    b, c = x + 1.0 - s, 1.0 / 1e-300
    d = 1.0 / b
    h = d
    for i in range(1, iters):
        an = -i * (i - s)
        b += 2.0
        d = an * d + b
        if abs(d) < tiny:
            d = tiny
        c = b + an / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break
    return math.exp(-x + s * math.log(x) - math.lgamma(s)) * h
