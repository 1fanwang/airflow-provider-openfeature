"""Tests for the fork-safe lazy provider wrapper."""

from __future__ import annotations

from openfeature.flag_evaluation import FlagResolutionDetails, Reason
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

from openfeature_airflow.providers.forksafe import ForkSafeProvider


class _Fake(AbstractProvider):
    def __init__(self) -> None:
        self.shut = False

    def get_metadata(self) -> Metadata:
        return Metadata(name="Fake")

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=True, reason=Reason.TARGETING_MATCH)

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value="v", reason=Reason.TARGETING_MATCH)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=7, reason=Reason.TARGETING_MATCH)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=1.5, reason=Reason.TARGETING_MATCH)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value={"a": 1}, reason=Reason.TARGETING_MATCH)

    def get_provider_hooks(self):
        return ["hook"]

    def shutdown(self) -> None:
        self.shut = True


def test_factory_deferred_until_first_eval():
    calls = []

    def factory():
        calls.append(1)
        return _Fake()

    p = ForkSafeProvider(factory)
    p.initialize()  # set_provider() calls this in the parent; must not build the real provider
    assert calls == []
    assert p.get_metadata().name == "ForkSafeProvider"
    assert p.get_provider_hooks() == []

    assert p.resolve_boolean_details("f", False).value is True
    assert calls == [1]  # built on first evaluation

    p.resolve_string_details("f", "d")
    assert calls == [1]  # built only once
    assert p.get_metadata().name == "Fake"  # delegates after build
    assert p.get_provider_hooks() == ["hook"]


def test_delegates_every_type():
    p = ForkSafeProvider(_Fake)
    assert p.resolve_boolean_details("f", False).value is True
    assert p.resolve_string_details("f", "d").value == "v"
    assert p.resolve_integer_details("f", 0).value == 7
    assert p.resolve_float_details("f", 0.0).value == 1.5
    assert p.resolve_object_details("f", None).value == {"a": 1}


def test_shutdown_is_safe_before_and_after_build():
    fake = _Fake()
    p = ForkSafeProvider(lambda: fake)
    p.shutdown()  # not built yet -> no-op, must not raise
    assert fake.shut is False

    p.resolve_boolean_details("f", False)  # build
    p.shutdown()
    assert fake.shut is True


def test_shutdown_tolerates_provider_without_shutdown():
    class _NoShutdown:
        def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
            return FlagResolutionDetails(value=False, reason=Reason.DEFAULT)

    p = ForkSafeProvider(_NoShutdown)
    p.resolve_boolean_details("f", False)  # build the duck-typed provider
    p.shutdown()  # real provider has no shutdown -> guarded, must not raise
