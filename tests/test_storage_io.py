from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from quorum.runtime.clock import FakeClock
from quorum.runtime.storage_io import (
    CrashInjected,
    DiskFullError,
    FaultProfile,
    IOFailureError,
    MemoryStorageIO,
    StorageIO,
)
from sim.trace import TraceRecorder

STEPS = ("write_temp", "fsync_temp", "rename", "fsync_dir")


def _io(faults: FaultProfile | None = None, seed: int = 0) -> MemoryStorageIO:
    return MemoryStorageIO(FakeClock(), node="n1", seed=seed, faults=faults)


def test_memory_storage_is_a_storage_io() -> None:
    io: StorageIO = _io()
    assert isinstance(io, StorageIO)


def test_fsync_makes_a_write_durable() -> None:
    io = _io()
    io.append("wal", b"entry-1")
    io.fsync("wal")
    io.crash()
    assert io.read("wal") == b"entry-1"


def test_unsynced_write_is_lost_on_crash() -> None:
    io = _io()
    io.append("wal", b"old")
    io.fsync("wal")
    io.append("wal", b"unsynced")
    assert io.read("wal") == b"oldunsynced"
    io.crash()
    assert io.read("wal") == b"old"


def test_write_with_no_durable_predecessor_disappears_entirely() -> None:
    io = _io()
    io.append("wal", b"only")
    io.crash()
    assert not io.exists("wal")
    with pytest.raises(FileNotFoundError):
        io.read("wal")


def test_overwrite_is_not_durable_until_fsync() -> None:
    io = _io()
    io.overwrite("meta", b"v1")
    io.fsync("meta")
    io.overwrite("meta", b"v2")
    io.crash()
    assert io.read("meta") == b"v1"


def test_unlink_and_list_files() -> None:
    io = _io()
    io.append("wal/1", b"a")
    io.append("wal/2", b"b")
    assert io.list_files("wal/") == ["wal/1", "wal/2"]
    io.unlink("wal/1")
    assert io.list_files("wal/") == ["wal/2"]


@pytest.mark.parametrize("step", STEPS)
def test_atomic_replace_is_all_or_nothing_at_each_crash_point(step: str) -> None:
    clock = FakeClock()
    recorder = TraceRecorder(clock, seed=0, config={"step": step})
    io = MemoryStorageIO(clock, node="n1", sink=recorder, faults=FaultProfile())
    io.atomic_replace("meta", b"old")
    io.set_faults(FaultProfile(interrupt_at_step=step))
    with pytest.raises(CrashInjected):
        io.atomic_replace("meta", b"new")
    io.crash()
    assert io.read("meta") in (b"old", b"new")
    assert "meta.tmp" not in io.list_files()
    steps = [event for event in recorder.events if event.details.get("step") is not None]
    assert len(steps) == len(STEPS) + STEPS.index(step) + 1


def test_atomic_replace_commits_through_the_last_step() -> None:
    io = _io()
    io.atomic_replace("meta", b"new")
    io.crash()
    assert io.read("meta") == b"new"


def test_torn_tail_keeps_a_prefix_of_the_unsynced_tail() -> None:
    io = _io(FaultProfile(torn_tail_on_crash=True, torn_tail_fraction=0.5))
    io.append("wal", b"old")
    io.fsync("wal")
    io.append("wal", b"12345678")
    io.crash()
    assert io.read("wal") == b"old1234"


def test_bit_flip_corrupts_a_read_but_not_the_stored_bytes() -> None:
    io = _io(FaultProfile(bit_flip_probability=1.0), seed=7)
    io.overwrite("page", b"abcdefgh")
    io.fsync("page")
    corrupted = io.read("page")
    assert corrupted != b"abcdefgh"
    io.set_faults(FaultProfile())
    assert io.read("page") == b"abcdefgh"


def test_bit_flip_is_reproducible_from_the_seed() -> None:
    def read(seed: int) -> bytes:
        storage = _io(FaultProfile(bit_flip_probability=1.0), seed=seed)
        storage.overwrite("page", b"abcdefgh")
        storage.fsync("page")
        return storage.read("page")

    assert read(11) == read(11)
    assert read(11) != read(12)


def test_disk_full_rejects_the_write_without_changing_state() -> None:
    io = _io(FaultProfile(disk_full_bytes=4))
    io.append("wal", b"abc")
    with pytest.raises(DiskFullError):
        io.append("wal", b"de")
    assert io.read("wal") == b"abc"


def test_fsync_failure_does_not_make_data_durable() -> None:
    io = _io(FaultProfile(fsync_failure_number=1))
    io.append("wal", b"data")
    with pytest.raises(IOFailureError):
        io.fsync("wal")
    io.crash()
    assert not io.exists("wal")
    io.append("wal", b"data")
    io.fsync("wal")
    io.crash()
    assert io.read("wal") == b"data"


def test_slow_fsync_charges_logical_time() -> None:
    clock = FakeClock()
    io = MemoryStorageIO(clock, faults=FaultProfile(slow_fsync_ticks=7))
    io.append("wal", b"data")
    io.fsync("wal")
    assert clock.now() == 7
    assert clock.advances == 1


def test_fault_profile_rejects_bad_values() -> None:
    with pytest.raises(ValueError):
        FaultProfile(torn_tail_fraction=-0.1)
    with pytest.raises(ValueError):
        FaultProfile(bit_flip_probability=1.5)
    with pytest.raises(ValueError):
        FaultProfile(fsync_failure_number=0)
    with pytest.raises(ValueError):
        FaultProfile(slow_fsync_ticks=-1)


_IO_OPS = st.lists(
    st.one_of(
        st.tuples(st.just("append"), st.binary(min_size=1, max_size=8)),
        st.just(("fsync",)),
        st.just(("crash",)),
    ),
    max_size=30,
)


@given(ops=_IO_OPS)
def test_visible_and_durable_states_match_a_two_bytearray_model(
    ops: list[tuple[Any, ...]],
) -> None:
    io = _io()
    durable = b""
    visible = b""
    for op in ops:
        if op[0] == "append":
            chunk = bytes(op[1])
            io.append("wal", chunk)
            visible += chunk
        elif op[0] == "fsync":
            io.fsync("wal")
            durable = visible
        else:
            io.crash()
            visible = durable
        assert io.durable_snapshot().get("wal", b"") == durable
        assert io.visible_snapshot().get("wal", durable) == visible
