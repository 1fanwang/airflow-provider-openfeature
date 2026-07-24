from __future__ import annotations

from openfeature import api

from openfeature_airflow.gate import flag_enabled, variant
from openfeature_airflow.providers.fractional import BoolFlag, FractionalProvider, VariantFlag
from openfeature_airflow.providers.inhouse import InHouseTreatmentProvider


class TestFlagEnabled:
    def test_true_when_rolled_out(self):
        api.set_provider(FractionalProvider(bool_flags={"f": BoolFlag(100)}))
        assert flag_enabled("f", "e") is True

    def test_default_when_unknown(self):
        api.set_provider(FractionalProvider())
        assert flag_enabled("missing", "e", default=True) is True
        assert flag_enabled("missing", "e", default=False) is False


class TestVariant:
    def test_resolves_and_defaults(self):
        api.set_provider(FractionalProvider(variant_flags={"m": VariantFlag([("a", 100)])}))
        assert variant("m", "e", "d") == "a"
        assert variant("missing", "e", "d") == "d"

    def test_attributes_flow_into_evaluation_context(self):
        api.set_provider(
            InHouseTreatmentProvider(
                string_flags={
                    "p": {
                        "segments": [{"attribute": "dag_id", "in": ["d1"], "variant": "on"}],
                        "default": "off",
                    }
                }
            )
        )
        assert variant("p", "d1:t", "x", dag_id="d1") == "on"
        assert variant("p", "d2:t", "x", dag_id="d2") == "off"
