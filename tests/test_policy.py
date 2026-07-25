from __future__ import annotations

from openfeature import api
from openfeature.flag_evaluation import FlagResolutionDetails, Reason
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

from openfeature_airflow.policy import FLAG_POOL, apply_placement
from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider


class _Task:
    def __init__(self, dag_id, task_id):
        self.dag_id = dag_id
        self.task_id = task_id
        self.pool = "default_pool"
        self.queue = "default"
        self.executor = None
        self.priority_weight = 1


def _register_pool_flag(canary):
    api.set_provider(
        InHouseTreatmentProvider(
            string_flags={
                FLAG_POOL: {
                    "segments": [{"attribute": "dag_id", "in": canary, "variant": "canary_pool"}],
                    "default": "default_pool",
                }
            }
        )
    )


class TestPlacementPolicy:
    def test_overrides_pool_for_cohort(self):
        _register_pool_flag(["d1"])
        t = _Task("d1", "x")
        apply_placement(t)
        assert t.pool == "canary_pool"

    def test_leaves_non_cohort_on_default(self):
        _register_pool_flag(["d1"])
        t = _Task("d2", "x")
        apply_placement(t)
        assert t.pool == "default_pool"

    def test_no_flag_configured_leaves_task_untouched(self):
        api.set_provider(InHouseTreatmentProvider(string_flags={}))
        t = _Task("d1", "x")
        apply_placement(t)
        assert t.pool == "default_pool"
        assert t.queue == "default"


def _register_multi(canary):
    api.set_provider(
        InHouseTreatmentProvider(
            string_flags={
                "airflow.task.pool": {
                    "segments": [{"attribute": "dag_id", "in": canary, "variant": "canary_pool"}],
                    "default": "default_pool",
                },
                "airflow.task.queue": {
                    "segments": [{"attribute": "dag_id", "in": canary, "variant": "kubernetes"}],
                },
            }
        )
    )


class TestQueuePlacement:
    def test_queue_overridden_for_cohort(self):
        _register_multi(["d1"])
        t = _Task("d1", "x")
        apply_placement(t)
        assert t.queue == "kubernetes"

    def test_queue_untouched_off_cohort(self):
        _register_multi(["d1"])
        t = _Task("d2", "x")
        apply_placement(t)
        assert t.queue == "default"


class TestTaskPolicyConfigGate:
    def test_hookimpl_noop_when_disabled(self, monkeypatch):
        from openfeature_airflow import policy

        monkeypatch.setattr(policy, "_policy_enabled", lambda: False)
        _register_pool_flag(["d1"])
        t = _Task("d1", "x")
        policy.task_policy(t)
        assert t.pool == "default_pool"

    def test_hookimpl_applies_when_enabled(self, monkeypatch):
        from openfeature_airflow import policy

        monkeypatch.setattr(policy, "_policy_enabled", lambda: True)
        _register_pool_flag(["d1"])
        t = _Task("d1", "x")
        policy.task_policy(t)
        assert t.pool == "canary_pool"

    def test_make_task_policy_factory_respects_enabled(self):
        from openfeature_airflow.policy import make_task_policy

        _register_pool_flag(["d1"])
        on = make_task_policy(enabled=True)
        t = _Task("d1", "x")
        on(t)
        assert t.pool == "canary_pool"

        off = make_task_policy(enabled=False)
        t2 = _Task("d1", "x")
        off(t2)
        assert t2.pool == "default_pool"


class _PriorityProvider(AbstractProvider):
    """Minimal provider that returns an integer for any flag (to exercise the priority lever)."""

    def __init__(self, weight):
        self._w = weight

    def get_metadata(self):
        return Metadata(name="priority")

    def get_provider_hooks(self):
        return []

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=self._w, reason=Reason.TARGETING_MATCH)

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)


class TestPriorityWeight:
    def test_priority_weight_set_from_flag(self):
        api.set_provider(_PriorityProvider(50))
        t = _Task("d1", "x")
        apply_placement(t)
        assert t.priority_weight == 50

    def test_priority_weight_untouched_without_int_flag(self):
        _register_pool_flag(["d1"])  # string-only provider -> integer resolves to the sentinel
        t = _Task("d1", "x")
        apply_placement(t)
        assert t.priority_weight == 1


class _ReadOnlyExecutorTask:
    """Mimics a mapped operator: `executor` is read-only and raises when assigned."""

    def __init__(self, dag_id, task_id):
        self.dag_id = dag_id
        self.task_id = task_id
        self.pool = "default_pool"
        self.queue = "default"
        self.priority_weight = 1

    @property
    def executor(self):
        return None

    @executor.setter
    def executor(self, value):
        raise AttributeError("can't set attribute 'executor'")


class TestReadOnlyAttributeIsSkipped:
    def test_readonly_executor_does_not_break_policy(self):
        api.set_provider(
            InHouseTreatmentProvider(
                string_flags={
                    "airflow.task.pool": {"default": "canary_pool"},
                    "airflow.task.executor": {"default": "LocalExecutor"},
                }
            )
        )
        t = _ReadOnlyExecutorTask("d1", "x")
        apply_placement(t)  # a read-only executor must not raise a policy error
        assert t.pool == "canary_pool"  # the other placements still apply


class TestCustomPlacementDimension:
    def test_custom_string_dimension_applies(self):
        from openfeature_airflow import policy

        policy.register_placement("airflow.task.spark_version", lambda t, v: setattr(t, "spark_version", v))
        try:
            api.set_provider(InHouseTreatmentProvider(string_flags={"airflow.task.spark_version": {"default": "3.5"}}))
            t = _Task("d1", "x")
            t.spark_version = None
            apply_placement(t)
            assert t.spark_version == "3.5"
        finally:
            policy._DIMENSIONS.pop()

    def test_custom_boolean_dimension_via_coerce(self):
        from openfeature_airflow import policy

        policy.register_placement(
            "airflow.task.enable_checkpoint", lambda t, v: setattr(t, "enable_checkpoint", v == "true")
        )
        try:
            api.set_provider(
                InHouseTreatmentProvider(string_flags={"airflow.task.enable_checkpoint": {"default": "true"}})
            )
            t = _Task("d1", "x")
            t.enable_checkpoint = False
            apply_placement(t)
            assert t.enable_checkpoint is True
        finally:
            policy._DIMENSIONS.pop()

    def test_custom_setter_that_raises_is_skipped(self):
        from openfeature_airflow import policy

        def _boom(task, value):
            raise RuntimeError("bad setter")

        policy.register_placement("airflow.task.explodes", _boom)
        try:
            api.set_provider(
                InHouseTreatmentProvider(
                    string_flags={
                        "airflow.task.explodes": {"default": "x"},
                        "airflow.task.pool": {"default": "canary_pool"},
                    }
                )
            )
            t = _Task("d1", "x")
            apply_placement(t)  # a raising custom setter must not break the policy
            assert t.pool == "canary_pool"  # later dimensions still apply
        finally:
            policy._DIMENSIONS.pop()

