"""The application state machine that Raft replicates (Raft §5.1, Figure 2).

Raft does not interpret commands; it only guarantees that every replica applies
the same committed sequence. The concrete implementation is an LSM/MVCC engine
from M8 onward.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class StateMachine(Protocol):
    def apply(self, command: str) -> bytes: ...

    def snapshot(self) -> bytes: ...

    def restore(self, image: bytes) -> None: ...
