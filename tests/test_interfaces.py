"""The injected boundaries are protocols; check that real implementations satisfy them."""

from __future__ import annotations

from quorum.runtime.clock import Clock, FakeClock, MutableClock
from quorum.runtime.state_machine import StateMachine
from quorum.runtime.storage_io import MemoryStorageIO, StorageIO
from quorum.runtime.trace import NullSink, TraceSink
from quorum.runtime.transport import Transport
from sim.network import SimNetwork
from sim.trace import TraceRecorder


class _KeyValue:
    """Small stand-in for the M8 storage-backed state machine."""

    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    def apply(self, command: str) -> bytes:
        key, _, value = command.partition("=")
        self.data[key] = value
        return value.encode("utf-8")

    def snapshot(self) -> bytes:
        return repr(sorted(self.data.items())).encode("utf-8")

    def restore(self, image: bytes) -> None:
        # Test double: real restore semantics belong to the M8 engine.
        self.data.clear()


def test_every_boundary_has_a_conforming_implementation() -> None:
    clock = FakeClock()
    recorder = TraceRecorder(clock, seed=0)
    network = SimNetwork(clock, seed=0)
    storage = MemoryStorageIO(clock)

    assert isinstance(clock, Clock)
    assert isinstance(clock, MutableClock)
    assert isinstance(storage, StorageIO)
    assert isinstance(recorder, TraceSink)
    assert isinstance(NullSink(), TraceSink)
    assert isinstance(network.transport("a"), Transport)
    assert isinstance(_KeyValue(), StateMachine)


def test_state_machine_protocol_is_structural() -> None:
    machine = _KeyValue()
    assert machine.apply("k=v") == b"v"
    assert machine.snapshot() == b"[('k', 'v')]"
    machine.restore(b"")
    assert machine.data == {}
