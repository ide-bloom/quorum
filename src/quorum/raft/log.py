"""The 1-indexed Raft log with a snapshot base offset.

``base_index`` is the index of the last entry folded into the snapshot, so
``entry_at(base_index + 1)`` is the first in-memory entry. Index 0 is a virtual
entry with term 0. # RAFT §5.3 (log structure), §7 (snapshot compaction)

The class owns no time, randomness or I/O: it is a plain deterministic container
that the Raft node (M2 onward) mutates on incoming events.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LogEntry:
    term: int
    command: str


class Log:
    """Entries are stored positionally; an entry's index is derived from the base."""

    def __init__(self) -> None:
        self._entries: list[LogEntry] = []
        self._base_index = 0
        self._base_term = 0

    @property
    def base_index(self) -> int:
        return self._base_index

    @property
    def base_term(self) -> int:
        return self._base_term

    def __len__(self) -> int:
        return len(self._entries)

    def entries(self) -> tuple[LogEntry, ...]:
        return tuple(self._entries)

    def last_index(self) -> int:
        return self._base_index + len(self._entries)

    def last_term(self) -> int:
        if not self._entries:
            return self._base_term
        return self._entries[-1].term

    def term_at(self, index: int) -> int | None:
        """Term at ``index``, or None when the index is compacted away or absent."""
        if index < 0:
            raise ValueError("log indexes are non-negative")
        if index == 0:
            return 0
        if index < self._base_index:
            return None
        if index == self._base_index:
            return self._base_term
        offset = index - self._base_index - 1
        if offset >= len(self._entries):
            return None
        return self._entries[offset].term

    def matches(self, index: int, term: int) -> bool:
        """The prev-log consistency check of AppendEntries. # RAFT §5.3"""
        return self.term_at(index) == term

    def entry_at(self, index: int) -> LogEntry:
        if index <= self._base_index or index > self.last_index():
            raise IndexError(
                f"index {index} unavailable (base={self._base_index}, last={self.last_index()})"
            )
        return self._entries[index - self._base_index - 1]

    def entries_from(self, index: int) -> tuple[LogEntry, ...]:
        if index <= self._base_index:
            raise IndexError(f"index {index} is covered by the snapshot at {self._base_index}")
        if index > self.last_index() + 1:
            raise IndexError(f"index {index} is past the end of the log")
        return tuple(self._entries[index - self._base_index - 1 :])

    def append(self, *entries: LogEntry) -> None:
        for entry in entries:
            if entry.term < self.last_term():
                raise ValueError("log terms must be nondecreasing")
            self._entries.append(entry)
        self.assert_invariants()

    def truncate_from(self, index: int) -> None:
        """Discard ``index`` and everything after it."""
        if index <= self._base_index:
            raise IndexError("cannot truncate a snapshot-covered prefix")
        if index > self.last_index():
            return
        del self._entries[index - self._base_index - 1 :]
        self.assert_invariants()

    def append_after(self, index: int, entries: Sequence[LogEntry]) -> None:
        """Low-level suffix replacement. # RAFT §5.3

        The M3 AppendEntries handler compares terms first and calls this only on
        a conflicting suffix, so matching entries are never truncated.
        """
        if index < self._base_index:
            raise IndexError("cannot overwrite a snapshot-covered prefix")
        if index > self.last_index():
            raise IndexError("cannot leave a gap in the log")
        if index < self.last_index():
            self.truncate_from(index + 1)
        self.append(*entries)

    def compact(self, index: int, term: int) -> None:
        """Discard entries <= ``index`` after locally snapshotting up to it."""
        if index < self._base_index:
            raise ValueError("cannot compact before the current base")
        if index > self.last_index():
            raise IndexError("cannot compact past the end of the log")
        if index > 0 and self.term_at(index) != term:
            raise ValueError("snapshot term does not match the log")
        del self._entries[: index - self._base_index]
        self._base_index = index
        self._base_term = term
        self.assert_invariants()

    def install_snapshot(self, index: int, term: int) -> None:
        """Receiver side of InstallSnapshot. # RAFT §7

        A matching suffix is kept; otherwise the whole in-memory log is dropped.
        A stale snapshot is ignored.
        """
        if index < self._base_index:
            return
        if index == self._base_index:
            if term != self._base_term:
                # DEVIATION: Raft §7 says to discard the whole log when a
                # snapshot conflicts. Log has no access to the state-machine
                # image, so accepting a different base term here without also
                # restoring that image would leave applied state inconsistent.
                # M5's InstallSnapshot receiver will restore the image and reset
                # the log directly instead of taking this path.
                raise ValueError("conflicting term for the current snapshot base")
            return
        if self.term_at(index) == term:
            self.compact(index, term)
        else:
            self._base_index = index
            self._base_term = term
            self._entries.clear()
            self.assert_invariants()

    def assert_invariants(self) -> None:
        """State Machine Safety's log-side preconditions. # RAFT §5.4"""
        if self._base_index < 0 or self._base_term < 0:
            raise AssertionError("snapshot base must be non-negative")
        previous = self._base_term
        for entry in self._entries:
            if entry.term < previous:
                raise AssertionError("log terms must be nondecreasing")
            previous = entry.term
