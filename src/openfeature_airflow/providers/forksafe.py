"""A provider wrapper that defers real-provider construction to first use, per process.

Some OpenFeature providers open a connection pool or start a background thread in ``__init__``. When
that happens before Airflow forks a task worker (Airflow 3.x's task supervisor is multi-threaded, and
some 2.x setups fork from a threaded parent too), the child inherits the resource and can deadlock on a
lock that no surviving thread will release. Building the real provider on the first flag evaluation --
which runs in the child, after the fork -- sidesteps that.

    from openfeature import api
    from openfeature.contrib.provider.flipt import FliptProvider
    from openfeature_airflow.providers.forksafe import ForkSafeProvider

    api.set_provider(ForkSafeProvider(lambda: FliptProvider(base_url="http://flipt:8080")))

The wrapped provider is built once per process, on the first ``resolve_*`` call. ``initialize`` is a
no-op on the wrapper so that ``api.set_provider`` does not build the real provider in the parent -- that
eager build is exactly what this avoids. A provider that needs an explicit ``initialize`` lifecycle in
the parent is not a fit; those tend to be the streaming clients that are not fork-safe anyway.

This does not make forking safe on its own. If the forking parent is multi-threaded for reasons outside
this provider (Airflow 3.x's LocalExecutor supervisor is, before any task code runs), the child can
still deadlock. It removes this provider's client as one source of that hazard, which is what you want
for the in-task evaluation path with a threaded or pooled backend.
"""

from __future__ import annotations

import threading
from typing import Callable

from openfeature.evaluation_context import EvaluationContext
from openfeature.provider import AbstractProvider
from openfeature.provider.metadata import Metadata


class ForkSafeProvider(AbstractProvider):
    """Builds the wrapped provider lazily, once per process, on first evaluation."""

    def __init__(self, factory: Callable[[], AbstractProvider]) -> None:
        self._factory = factory
        self._real: AbstractProvider | None = None
        self._lock = threading.Lock()
        self._init_ctx: EvaluationContext | None = None

    def _ensure(self) -> AbstractProvider:
        # Double-checked locking: the fast path skips the lock once built; the lock keeps two threads
        # from constructing two real providers on the first concurrent evaluation.
        if self._real is None:
            with self._lock:
                if self._real is None:
                    real = self._factory()
                    self._initialize(real, self._init_ctx or EvaluationContext())
                    self._real = real
        return self._real

    @staticmethod
    def _initialize(real: AbstractProvider, ctx: EvaluationContext) -> None:
        # Run the real provider's lifecycle now, in this process, after construction.
        init = getattr(real, "initialize", None)
        if not callable(init):
            return
        try:
            init(ctx)
        except TypeError:
            init()

    def initialize(self, evaluation_context=None) -> None:
        # Deliberately do not build the real provider: set_provider() calls this in the parent, and an
        # eager build there is what we are avoiding. Keep the context for the lazy build.
        self._init_ctx = evaluation_context

    def shutdown(self) -> None:
        if self._real is not None:
            shutdown = getattr(self._real, "shutdown", None)
            if callable(shutdown):
                shutdown()

    def get_metadata(self) -> Metadata:
        return self._real.get_metadata() if self._real is not None else Metadata(name="ForkSafeProvider")

    def get_provider_hooks(self):
        return self._real.get_provider_hooks() if self._real is not None else []

    def resolve_boolean_details(self, flag_key, default_value, evaluation_context=None):
        return self._ensure().resolve_boolean_details(flag_key, default_value, evaluation_context)

    def resolve_string_details(self, flag_key, default_value, evaluation_context=None):
        return self._ensure().resolve_string_details(flag_key, default_value, evaluation_context)

    def resolve_integer_details(self, flag_key, default_value, evaluation_context=None):
        return self._ensure().resolve_integer_details(flag_key, default_value, evaluation_context)

    def resolve_float_details(self, flag_key, default_value, evaluation_context=None):
        return self._ensure().resolve_float_details(flag_key, default_value, evaluation_context)

    def resolve_object_details(self, flag_key, default_value, evaluation_context=None):
        return self._ensure().resolve_object_details(flag_key, default_value, evaluation_context)
