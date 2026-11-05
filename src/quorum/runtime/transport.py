"""The only network boundary the core is allowed to cross.

A transport moves opaque frames, not Python objects: serialization belongs to
the caller. The simulator and (in M17) the TCP implementation both satisfy this
protocol, so a node is identical in either environment.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

NodeId = str


@runtime_checkable
class Transport(Protocol):
    def send(self, destination: NodeId, frame: bytes) -> None: ...
