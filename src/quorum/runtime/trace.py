"""Trace sink interface used by the core.

The concrete recorder lives in :mod:`sim.trace`. The core only knows it can hand
events to a sink, which keeps the dependency direction ``core <- sim`` rather
than the other way around.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class TraceSink(Protocol):
    def record(
        self,
        kind: str,
        *,
        node: str | None = None,
        peer: str | None = None,
        **details: object,
    ) -> None: ...


class NullSink:
    """Drop events; used when a component runs without tracing."""

    def record(
        self,
        kind: str,
        *,
        node: str | None = None,
        peer: str | None = None,
        **details: object,
    ) -> None:
        return None
