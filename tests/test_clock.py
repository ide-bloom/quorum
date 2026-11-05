from __future__ import annotations

import pytest

from quorum.runtime.clock import Clock, FakeClock, MutableClock


def test_clock_starts_at_zero_and_only_moves_when_advanced() -> None:
    clock = FakeClock()
    assert clock.now() == 0
    clock.advance(7)
    assert clock.now() == 7
    assert clock.advances == 1
    assert clock.now() == 7


def test_clock_rejects_backwards_time_and_negative_start() -> None:
    clock = FakeClock(10)
    with pytest.raises(ValueError):
        clock.advance(-1)
    with pytest.raises(ValueError):
        FakeClock(-1)


def test_fake_clock_satisfies_both_protocols() -> None:
    clock: MutableClock = FakeClock()
    plain: Clock = clock
    assert isinstance(plain, Clock)
    assert isinstance(clock, MutableClock)
