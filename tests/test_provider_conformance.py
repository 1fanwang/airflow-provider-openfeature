"""OpenFeature contract conformance: every bundled provider, every value type, through the client.

Each provider is driven via the real OpenFeature client (``api.get_client().get_*_details``), not by
calling ``resolve_*_details`` directly, so a wrong ``resolve_*_details`` signature (the SDK calls them
with a ``flag_key=`` keyword) or a broken value-type path is caught. Per the OpenFeature contract, an
unknown flag returns the caller's default for every type with a non-error reason. Asserting on the
reason catches a broken resolve whether the SDK re-raises or downgrades it to an ERROR reason.
"""

from __future__ import annotations

import pytest
from openfeature import api
from openfeature.evaluation_context import EvaluationContext
from openfeature.flag_evaluation import Reason

from openfeature_airflow.providers.fractional import BoolFlag, FractionalProvider, VariantFlag
from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider
from openfeature_airflow.providers.unleash import UnleashProvider


class _FakeUnleash:
    def is_enabled(self, feature, context, default=False):
        return False

    def get_variant(self, feature, context):
        return {"name": "disabled", "enabled": False}


class _FakeStatsig:
    def check_gate(self, user, gate):
        return False


def _build_providers():
    provs = [
        ("fractional", FractionalProvider(bool_flags={"b": BoolFlag(50)},
                                          variant_flags={"s": VariantFlag([("a", 100)])})),
        ("inhouse", InHouseTreatmentProvider(string_flags={"s": {"default": "on"}})),
        ("unleash", UnleashProvider(_FakeUnleash())),
    ]
    try:
        import growthbook  # noqa: F401

        from openfeature_airflow.providers.growthbook import GrowthBookProvider
        provs.append(("growthbook", GrowthBookProvider(features={})))
    except Exception:
        pass
    try:
        import statsig  # noqa: F401

        from openfeature_airflow.providers.statsig import StatsigProvider
        provs.append(("statsig", StatsigProvider(_FakeStatsig(), gate_map={})))
    except Exception:
        pass
    return provs


_PROVIDERS = _build_providers()
_IDS = [name for name, _ in _PROVIDERS]
_CTX = EvaluationContext(targeting_key="dag_x:task_y", attributes={"dag_id": "dag_x"})


@pytest.mark.parametrize("provider", [p for _, p in _PROVIDERS], ids=_IDS)
class TestOpenFeatureConformance:
    def test_every_value_type_resolves_without_error(self, provider):
        # every resolve_*_details is reachable through the client for every type; a wrong signature or
        # a broken type path shows up as a raise or an ERROR reason
        api.set_provider(provider)
        c = api.get_client()
        cases = [
            (c.get_boolean_details, True),
            (c.get_string_details, "dflt"),
            (c.get_integer_details, 7),
            (c.get_float_details, 1.5),
            (c.get_object_details, {"k": "v"}),
        ]
        for get_details, default in cases:
            d = get_details("unknown.flag", default, _CTX)
            assert d.reason != Reason.ERROR, (get_details.__name__, d.reason, d.error_message)

    def test_unimplemented_types_fall_back_to_caller_default(self, provider):
        # no bundled provider resolves numbers or objects, so those must return the caller default
        api.set_provider(provider)
        c = api.get_client()
        assert c.get_integer_value("x", 42, _CTX) == 42
        assert c.get_float_value("x", 2.5, _CTX) == 2.5
        assert c.get_object_value("x", {"k": 1}, _CTX) == {"k": 1}


def test_registry_providers_return_default_for_unknown_flag():
    # registry-based providers know their flags, so an unknown flag returns the caller default; gate-based
    # backends (Unleash/GrowthBook/Statsig) instead reflect the backend's answer (off), so this contract
    # is asserted only where it applies
    for provider in (FractionalProvider(), InHouseTreatmentProvider()):
        api.set_provider(provider)
        c = api.get_client()
        assert c.get_boolean_value("nope", True, _CTX) is True
        assert c.get_string_value("nope", "sentinel", _CTX) == "sentinel"


def test_known_flag_resolves_a_real_value_through_the_client():
    api.set_provider(FractionalProvider(variant_flags={"m": VariantFlag([("treatment", 100)])}))
    assert api.get_client().get_string_value("m", "control", _CTX) == "treatment"
    api.set_provider(InHouseTreatmentProvider(string_flags={"m": {"default": "on"}}))
    assert api.get_client().get_string_value("m", "off", _CTX) == "on"
