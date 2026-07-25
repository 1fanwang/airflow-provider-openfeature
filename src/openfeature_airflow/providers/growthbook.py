"""OpenFeature provider backed by the GrowthBook SDK (evaluated locally). Install: ``pip install growthbook``."""

from __future__ import annotations

from openfeature.flag_evaluation import FlagResolutionDetails, Reason
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata

from openfeature_airflow.providers.fractional import entity_of


class GrowthBookProvider(AbstractProvider):
    """Resolve flags via a GrowthBook instance.

    Provide either an in-process ``features`` payload, or ``api_host`` + ``client_key`` to fetch the
    payload from a running GrowthBook API/proxy over HTTP (the production path).

    :param features: a GrowthBook features payload (in-process).
    :param api_host: GrowthBook API/proxy base URL; with ``client_key`` the SDK fetches over HTTP.
    :param client_key: GrowthBook SDK client key.
    :param hash_attribute: the eval-context attribute GrowthBook buckets on (default ``id``).
    """

    def __init__(
        self, features: dict | None = None, api_host: str = "", client_key: str = "", hash_attribute: str = "id",
        on_track=None,
    ) -> None:
        from growthbook import GrowthBook

        if api_host and client_key:
            self._gb = GrowthBook(api_host=api_host, client_key=client_key)
            self._gb.load_features()  # real HTTP GET {api_host}/api/features/{client_key}
        elif features is not None:
            self._gb = GrowthBook(features=features)
        else:
            raise ValueError("GrowthBookProvider needs either features= or api_host+client_key")
        self._hash_attr = hash_attribute
        self._on_track = on_track  # callback(metric, entity, value, attrs): write the outcome to your warehouse

    def get_metadata(self) -> Metadata:
        return Metadata(name="GrowthBookProvider")

    def get_provider_hooks(self):
        return []

    def _eval(self, flag_key, ctx):
        attrs = {self._hash_attr: entity_of(ctx)}
        if ctx is not None and ctx.attributes:
            attrs.update(ctx.attributes)
        self._gb.set_attributes(attrs)
        return self._gb.eval_feature(flag_key)

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        r = self._eval(flag_key, evaluation_context)
        return FlagResolutionDetails(value=bool(r.on), reason=Reason.TARGETING_MATCH)

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        r = self._eval(flag_key, evaluation_context)
        if r.value is None:
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)
        return FlagResolutionDetails(value=str(r.value), variant=str(r.value), reason=Reason.TARGETING_MATCH)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        r = self._eval(flag_key, evaluation_context)
        try:
            return FlagResolutionDetails(value=int(r.value), reason=Reason.TARGETING_MATCH)
        except (TypeError, ValueError):
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        r = self._eval(flag_key, evaluation_context)
        try:
            return FlagResolutionDetails(value=float(r.value), reason=Reason.TARGETING_MATCH)
        except (TypeError, ValueError):
            return FlagResolutionDetails(value=default_value, reason=Reason.DEFAULT)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        r = self._eval(flag_key, evaluation_context)
        return FlagResolutionDetails(
            value=r.value if r.value is not None else default_value, reason=Reason.TARGETING_MATCH
        )

    def track(self, tracking_event_name, evaluation_context=None, tracking_event_details=None):
        """Hand the outcome to your GrowthBook tracking callback (GrowthBook measures in the warehouse)."""
        if self._on_track is None:
            return
        try:
            self._on_track(
                tracking_event_name,
                entity_of(evaluation_context),
                getattr(tracking_event_details, "value", None),
                getattr(tracking_event_details, "attributes", None) or {},
            )
        except Exception:
            pass

