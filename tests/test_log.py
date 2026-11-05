from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from quorum.raft.log import Log, LogEntry


def _entry(term: int, command: str) -> LogEntry:
    return LogEntry(term=term, command=command)


def test_empty_log_uses_virtual_index_zero() -> None:
    log = Log()
    assert log.last_index() == 0
    assert log.last_term() == 0
    assert log.term_at(0) == 0
    assert log.matches(0, 0)
    assert log.term_at(1) is None
    assert log.entries() == ()
    assert log.entries_from(1) == ()
    with pytest.raises(IndexError):
        log.entry_at(1)


def test_append_is_one_indexed_and_terms_must_not_decrease() -> None:
    log = Log()
    log.append(_entry(1, "a"), _entry(2, "b"))
    assert log.last_index() == 2
    assert log.last_term() == 2
    assert log.entry_at(1) == _entry(1, "a")
    assert log.entries_from(2) == (_entry(2, "b"),)
    with pytest.raises(ValueError):
        log.append(_entry(1, "c"))
    assert log.last_index() == 2


def test_truncate_only_removes_the_suffix() -> None:
    log = Log()
    log.append(_entry(1, "a"), _entry(1, "b"), _entry(2, "c"))
    log.truncate_from(2)
    assert log.entries() == (_entry(1, "a"),)
    log.truncate_from(99)
    assert log.last_index() == 1
    with pytest.raises(IndexError):
        log.truncate_from(0)


def test_append_after_replaces_conflicting_suffix_but_keeps_prefix() -> None:
    log = Log()
    log.append(_entry(1, "a"), _entry(1, "old"), _entry(2, "old2"))
    log.append_after(1, [_entry(3, "new"), _entry(4, "new2")])
    assert log.entries() == (
        _entry(1, "a"),
        _entry(3, "new"),
        _entry(4, "new2"),
    )
    with pytest.raises(IndexError):
        log.append_after(9, [_entry(5, "gap")])


def test_compaction_moves_the_base_and_hides_old_entries() -> None:
    log = Log()
    log.append(_entry(1, "a"), _entry(2, "b"), _entry(3, "c"), _entry(3, "d"))
    log.compact(2, 2)
    assert log.base_index == 2
    assert log.base_term == 2
    assert log.term_at(2) == 2
    assert log.term_at(1) is None
    assert log.matches(2, 2)
    assert log.entries() == (_entry(3, "c"), _entry(3, "d"))
    assert log.entry_at(3) == _entry(3, "c")
    assert log.last_index() == 4
    with pytest.raises(IndexError):
        log.entry_at(2)
    with pytest.raises(IndexError):
        log.entries_from(2)
    with pytest.raises(IndexError):
        log.truncate_from(2)


def test_compaction_rejects_wrong_term_and_out_of_range_index() -> None:
    log = Log()
    log.append(_entry(1, "a"), _entry(2, "b"))
    with pytest.raises(ValueError):
        log.compact(2, 1)
    with pytest.raises(IndexError):
        log.compact(3, 2)
    assert log.last_index() == 2


def test_install_snapshot_keeps_a_matching_suffix() -> None:
    log = Log()
    log.append(_entry(1, "a"), _entry(2, "b"), _entry(3, "c"))
    log.install_snapshot(2, 2)
    assert (log.base_index, log.base_term) == (2, 2)
    assert log.entries() == (_entry(3, "c"),)


def test_install_snapshot_discards_a_conflicting_suffix_and_ignores_stale_ones() -> None:
    log = Log()
    log.append(_entry(1, "a"), _entry(2, "b"), _entry(3, "c"))
    log.install_snapshot(2, 9)
    assert (log.base_index, log.base_term) == (2, 9)
    assert log.entries() == ()
    assert log.last_index() == 2
    log.install_snapshot(1, 1)
    assert (log.base_index, log.base_term) == (2, 9)


def test_install_snapshot_ahead_of_the_log_advances_the_base() -> None:
    log = Log()
    log.append(_entry(1, "a"))
    log.install_snapshot(10, 5)
    assert log.last_index() == 10
    assert log.last_term() == 5
    assert log.matches(10, 5)
    assert log.entries() == ()
    log.append(_entry(5, "after"))
    assert log.entry_at(11) == _entry(5, "after")


def test_conflicting_term_for_current_base_is_rejected() -> None:
    log = Log()
    log.append(_entry(1, "a"), _entry(1, "b"), _entry(1, "c"), _entry(2, "d"))
    log.compact(4, 2)
    with pytest.raises(ValueError):
        log.install_snapshot(4, 3)


_OPS = st.lists(
    st.one_of(
        st.tuples(st.just("append"), st.integers(1, 6), st.text(max_size=4)),
        st.tuples(st.just("truncate"), st.integers(0, 30)),
    ),
    max_size=40,
)


@given(ops=_OPS)
def test_log_matches_a_plain_list_model(ops: list[tuple[Any, ...]]) -> None:
    log = Log()
    model: list[LogEntry] = []
    for op in ops:
        if op[0] == "append":
            term = max(int(op[1]), model[-1].term) if model else int(op[1])
            entry = _entry(term, str(op[2]))
            log.append(entry)
            model.append(entry)
        else:
            index = int(op[1])
            log.truncate_from(index + 1)
            del model[index:]
        assert log.entries() == tuple(model)
        assert log.last_index() == len(model)
        assert log.last_term() == (model[-1].term if model else 0)
        log.assert_invariants()
