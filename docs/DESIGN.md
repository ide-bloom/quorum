# Design notes

This file grows with the milestones. It records the decisions that are easy to
get wrong later: what is injected, when a write is allowed to be acknowledged,
and which parts of the system are allowed to know about time, randomness, disks
or the network.

## Status

Tier 1, M1 (foundations). Anything not described here has not been built yet.

## Layering

```
tests / sim  ->  quorum.raft / quorum.storage / ...  ->  quorum.runtime
```

The core never imports from `sim`. It sees only protocols in
`quorum.runtime`: `Clock`, `Transport`, `StorageIO`, `StateMachine` and
`TraceSink`. The simulator supplies `FakeClock`, `SimNetwork`/`SimTransport`,
`MemoryStorageIO` and `TraceRecorder`; M17 supplies a TCP transport and a real
file-backed `StorageIO`. A Raft node (M2) is a pure function of its input
events: events in, outputs out.

`sim/` is top-level rather than under `src/` (as specified in the project
layout), so `pyproject.toml` adds both `src` and `.` to the import path.
Two small files were added to the layout: `runtime/state_machine.py` holds the
application state machine protocol, and `runtime/trace.py` holds `TraceSink`
and `NullSink`. The latter is what keeps the dependency direction one-way.

## Determinism rules

1. Logical time is an integer number of ticks (one tick = 1 microsecond by
   convention). Only the simulation event loop advances it. No core component
   reads `time.*`.
2. Randomness comes from `random.Random(seed)`. `SimNetwork` and
   `MemoryStorageIO` each own a seeded RNG. No unseeded randomness, no
   `hash()`-dependent iteration in a decision.
3. `SimNetwork` deliveries are ordered by `(deliver_at, sequence)`, where
   `sequence` is a strictly increasing counter. Ties therefore never depend on
   dict or set order.
4. Every network decision (drop, duplicate, delay, reorder) is made at send
   time and recorded as a trace event with a reason. A replay of the same seed
   and script produces a byte-identical trace.

## Storage and persistence ordering rules

These are the rules the simulator enforces via `MemoryStorageIO`. M4 adds the
Raft-specific ones (currentTerm/votedFor/log) and tests each crash point.

| # | Rule | Mechanism |
|---|---|---|
| P1 | A write is visible to the process immediately but is not durable until `fsync(path)` returns. | Two byte-images per path: visible and durable. |
| P2 | A power loss discards all unsynced state. | `MemoryStorageIO.crash()` clears visible state. |
| P3 | Metadata updates use temp -> fsync(temp) -> rename -> fsync(directory), never an in-place overwrite. | `atomic_replace()`; `interrupt_at_step` can cut power at each step. |
| P4 | A torn write may leave a valid prefix of the unsynced tail, never a hole. | `torn_tail_on_crash` keeps a deterministic prefix. |
| P5 | A failed `fsync` must not be treated as success, and slow `fsync` must be visible to the injected clock. | `fsync_failure_number`, `slow_fsync_ticks`. |
| P6 | A full disk rejects the write rather than silently truncating it. | `disk_full_bytes`. |
| P7 | A corrupted read returns wrong bytes; callers detect it with their own checksums. | `bit_flip_probability`. |

**Simplification (until M4/M8):** `unlink` takes effect immediately instead of
requiring a directory fsync, and a power loss discards leftover `.tmp` files
instead of leaving them for recovery to clean up. Neither affects M1 tests.

The general acknowledge rule, which M4 will enforce with crash tests, is:
*persist (P1/P3) before replying to a client or to another node*. A reply that
precedes the fsync is a bug; the mutation suite in M7 will move an fsync after
its reply to prove the tests catch it.

## Wire format

One message is a 4-byte big-endian length followed by compact UTF-8 JSON. A
`type` field selects one of the RPC dataclasses; log entries are JSON objects;
snapshot chunks are base64 wrapped so the wire remains JSON. Frames are
decoded strictly: a truncated frame, an unknown type or a length mismatch is a
`ValueError`, not an empty message. The `pre_vote` field is present from M1 but
unused until M2, to avoid changing the wire format mid-project.

## Trace format

`Trace.to_json()` writes `{"meta": {...}, "events": [...]}`. `meta` carries
`format`, `seed`, `config` and `event_count`. Each event carries `seq`,
`time` (logical), `kind`, `node`, `peer` and JSON-safe `details`. Kinds used in
M1: `send`, `receive`, `drop`, `crash`, `restart`, `storage`, `config`. M2 adds
`timer` and `role_change`; later milestones add `commit`, `apply` and
transaction phases. Every `send` has a terminal outcome (`receive` or `drop`).

## Log model

The Raft log is 1-indexed. `index 0` is a virtual entry with term 0.
`base_index`/`base_term` describe the last entry folded into a snapshot, so the
first in-memory entry is `base_index + 1`. `matches(index, term)` implements
the AppendEntries consistency check; `compact()` is the leader-side snapshot
path and `install_snapshot()` is the receiver-side path (a matching suffix is
kept, per Raft section 7).

One deliberate deviation is marked with `# DEVIATION:` in `log.py`: if an
incoming snapshot has the same `last_included_index` as the current base but a
different term, `Log.install_snapshot()` raises instead of silently discarding
the log. Raft section 7 would discard the log, but the `Log` class cannot also
restore the state-machine image, so doing it here would desynchronize the
applied state. M5's InstallSnapshot receiver owns both operations and will
handle the conflicting-base case there. This is flagged rather than hidden.

## Known limitations

- No Raft node, no elections, no replication yet (M2/M3).
- No WAL, SSTables, MVCC, sharding or transactions yet.
- `MemoryStorageIO` is a file abstraction over bytes, not a filesystem with
  directory entries, permissions or partial sector semantics.
- No benchmarks, chaos totals, mutation results or model-checking results are
  reported anywhere in this repository yet. They will be added only when
  actually measured.
