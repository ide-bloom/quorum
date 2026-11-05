"""Injectable time sources.

No component in the core reads ``time.time`` or ``time.monotonic``. A node sees
time only through the injected :class:`Clock`, so a simulation replays an exact
sequence of logical instants from a seed.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Integer logical time. One tick is one microsecond by convention."""

    def now(self) -> int: ...


@runtime_checkable
class MutableClock(Clock, Protocol):
    """A clock the simulator may advance (event loops and slow I/O use this)."""

    def advance(self, ticks: int) -> None: ...


class FakeClock:
    """Logical clock that only moves when the simulator says so."""

    def __init__(self, start: int = 0) -> None:
        if start < 0:
            raise ValueError("logical time cannot be negative")
        self._now = start
        self._advances = 0

    def now(self) -> int:
        return self._now

    def advance(self, ticks: int) -> None:
        if ticks < 0:
            raise ValueError("logical time cannot go backwards")
        self._now += ticks
        self._advances += 1

    @property
    def advances(self) -> int:
        return self._advances
