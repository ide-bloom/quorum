"""File-system boundary and its fault-injecting in-memory implementation.

Every disk access in Quorum goes through :class:`StorageIO`. The simulator uses
:class:`MemoryStorageIO`, which keeps two byte-images per path: *visible* (what
the running process reads) and *durable* (what survives a power loss). That one
distinction is what makes "persist before reply" testable: a node that replies
before calling ``fsync`` loses the write when the power is cut.

Faults are seeded and traced. Nothing here reads the wall clock or makes a
decision that a replay could not reproduce.
"""

from __future__ import annotations

import dataclasses
import random
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from quorum.runtime.clock import MutableClock
from quorum.runtime.trace import NullSink, TraceSink


class StorageError(Exception):
    """Base class for injected storage faults."""


class DiskFullError(StorageError):
    """A write was rejected because the configured capacity was exceeded."""


class IOFailureError(StorageError):
    """An fsync failed with an I/O error."""


class CrashInjected(StorageError):
    """Control-flow exception: power was cut at a named I/O step.

    Raised by ``atomic_replace`` so a test can stop at each persistence point,
    then call ``crash()`` and recover from whatever reached durable storage.
    """


@runtime_checkable
class StorageIO(Protocol):
    def read(self, path: str) -> bytes: ...

    def append(self, path: str, data: bytes) -> None: ...

    def overwrite(self, path: str, data: bytes) -> None: ...

    def fsync(self, path: str) -> None: ...

    def atomic_replace(self, path: str, data: bytes) -> None: ...

    def exists(self, path: str) -> bool: ...

    def unlink(self, path: str) -> None: ...

    def list_files(self, prefix: str = "") -> list[str]: ...


@runtime_checkable
class FaultInjectingStorageIO(StorageIO, Protocol):
    def crash(self) -> None: ...


@dataclass(frozen=True, slots=True)
class FaultProfile:
    """Fault modes available to the chaos runner.

    ``interrupt_at_step`` names one of the four steps of ``atomic_replace``:
    ``write_temp``, ``fsync_temp``, ``rename``, ``fsync_dir``.
    """

    lose_unsynced_on_crash: bool = True
    torn_tail_on_crash: bool = False
    torn_tail_fraction: float = 0.5
    bit_flip_probability: float = 0.0
    fsync_failure_number: int | None = None
    disk_full_bytes: int | None = None
    slow_fsync_ticks: int = 0
    interrupt_at_step: str | None = None

    def __post_init__(self) -> None:
        for name in ("torn_tail_fraction", "bit_flip_probability"):
            value = float(getattr(self, name))
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.slow_fsync_ticks < 0:
            raise ValueError("slow_fsync_ticks cannot be negative")
        if self.fsync_failure_number is not None and self.fsync_failure_number < 1:
            raise ValueError("fsync_failure_number is 1-based")
        if self.disk_full_bytes is not None and self.disk_full_bytes < 0:
            raise ValueError("disk_full_bytes cannot be negative")


class MemoryStorageIO:
    """In-memory files with explicit durability and seeded fault injection.

    Crash semantics follow the persistence rules in ``docs/DESIGN.md``:
    unsynced writes are lost, a torn tail may keep a prefix, and temporary files
    from an interrupted ``atomic_replace`` are discarded. Directory-fsync
    semantics for ``unlink`` are simplified: unlink takes effect immediately.
    """

    def __init__(
        self,
        clock: MutableClock,
        *,
        node: str = "node",
        seed: int = 0,
        sink: TraceSink | None = None,
        faults: FaultProfile | None = None,
    ) -> None:
        self._clock = clock
        self._node = node
        self._rng = random.Random(seed)
        self._sink: TraceSink = sink if sink is not None else NullSink()
        self._faults = faults if faults is not None else FaultProfile()
        self._durable: dict[str, bytes] = {}
        self._visible: dict[str, bytearray] = {}
        self._fsync_calls = 0

    @property
    def faults(self) -> FaultProfile:
        return self._faults

    @property
    def fsync_calls(self) -> int:
        return self._fsync_calls

    def set_faults(self, faults: FaultProfile) -> None:
        self._faults = faults
        self._sink.record(
            "config",
            node=self._node,
            change="storage_faults",
            profile=dataclasses.asdict(faults),
        )

    def durable_snapshot(self) -> dict[str, bytes]:
        """Introspection for the simulator; never used by the core."""
        return dict(self._durable)

    def visible_snapshot(self) -> dict[str, bytes]:
        """Introspection for the simulator; never used by the core."""
        return {path: bytes(buf) for path, buf in self._visible.items()}

    def read(self, path: str) -> bytes:
        data = self._visible_bytes(path)
        probability = self._faults.bit_flip_probability
        if data and probability > 0.0 and self._rng.random() < probability:
            flipped = bytearray(data)
            index = self._rng.randrange(len(data))
            flipped[index] ^= 1 << self._rng.randrange(8)
            self._record(op="read", path=path, size=len(data), fault="bit_flip")
            return bytes(flipped)
        self._record(op="read", path=path, size=len(data))
        return data

    def append(self, path: str, data: bytes) -> None:
        self._check_capacity(len(data))
        buf = self._visible.setdefault(path, bytearray(self._durable.get(path, b"")))
        buf.extend(data)
        self._record(op="append", path=path, size=len(data))

    def overwrite(self, path: str, data: bytes) -> None:
        self._check_capacity(len(data))
        self._visible[path] = bytearray(data)
        self._record(op="overwrite", path=path, size=len(data))

    def fsync(self, path: str) -> None:
        self._fsync_calls += 1
        if self._faults.fsync_failure_number == self._fsync_calls:
            self._record(op="fsync", path=path, fault="io_error")
            raise IOFailureError(f"injected fsync failure #{self._fsync_calls} on {path}")
        if path in self._visible:
            self._durable[path] = bytes(self._visible[path])
        fault: str | None = None
        if self._faults.slow_fsync_ticks > 0:
            self._clock.advance(self._faults.slow_fsync_ticks)
            fault = "slow_fsync"
        self._record(op="fsync", path=path, fault=fault)

    def atomic_replace(self, path: str, data: bytes) -> None:
        """temp write -> fsync temp -> rename -> fsync directory.

        The directory sync is what makes the rename durable; a crash between
        rename and that sync is the interesting case for M4.
        """
        self._check_capacity(len(data))
        temp = f"{path}.tmp"
        self._step("write_temp", path)
        self._visible[temp] = bytearray(data)
        self._step("fsync_temp", path)
        self._durable[temp] = bytes(self._visible[temp])
        self._step("rename", path)
        self._durable[path] = self._durable.pop(temp)
        self._visible.pop(temp, None)
        self._visible[path] = bytearray(self._durable[path])
        self._step("fsync_dir", path)
        self._record(op="atomic_replace", path=path, size=len(data))

    def exists(self, path: str) -> bool:
        return path in self._visible or path in self._durable

    def unlink(self, path: str) -> None:
        self._durable.pop(path, None)
        self._visible.pop(path, None)
        self._record(op="unlink", path=path)

    def list_files(self, prefix: str = "") -> list[str]:
        paths = sorted(set(self._durable) | set(self._visible))
        return [path for path in paths if path.startswith(prefix)]

    def crash(self) -> None:
        """Power loss: drop unsynced state, then apply any torn-tail damage."""
        if self._faults.torn_tail_on_crash:
            self._keep_torn_tails()
        if self._faults.lose_unsynced_on_crash:
            self._visible.clear()
        for path in [p for p in self._durable if p.endswith(".tmp")]:
            del self._durable[path]
        self._sink.record("crash", node=self._node, component="storage")

    def _step(self, name: str, path: str) -> None:
        self._record(op="atomic_replace_step", path=path, step=name)
        if self._faults.interrupt_at_step == name:
            raise CrashInjected(f"power loss during {name}")

    def _keep_torn_tails(self) -> None:
        for path in sorted(set(self._durable) | set(self._visible)):
            durable = self._durable.get(path, b"")
            visible = self._visible.get(path, bytearray())
            if len(visible) <= len(durable):
                continue
            extra = bytes(visible)[len(durable) :]
            keep = max(1, int(len(extra) * self._faults.torn_tail_fraction))
            self._durable[path] = durable + extra[:keep]
            self._record(op="torn_tail", path=path, kept=keep, lost=len(extra) - keep)

    def _visible_bytes(self, path: str) -> bytes:
        if path in self._visible:
            return bytes(self._visible[path])
        if path in self._durable:
            return self._durable[path]
        raise FileNotFoundError(path)

    def _used_bytes(self) -> int:
        paths = sorted(set(self._durable) | set(self._visible))
        return sum(len(self._visible_bytes(path)) for path in paths)

    def _check_capacity(self, additional: int) -> None:
        limit = self._faults.disk_full_bytes
        if limit is None:
            return
        used = self._used_bytes()
        if used + additional > limit:
            self._record(op="capacity", used=used, limit=limit, fault="disk_full")
            raise DiskFullError(f"disk full: {used} + {additional} > {limit}")

    def _record(self, **details: object) -> None:
        self._sink.record("storage", node=self._node, peer=None, **details)
