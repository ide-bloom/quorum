# Quorum

A sharded, transactional, chaos-verified distributed database built from
scratch in Python 3.11+: Raft consensus, an LSM/MVCC storage engine, multi-Raft
sharding, distributed ACID transactions, and a verification layer that tries
hard to prove the whole thing wrong.

## The verification story (current status: not started)

The point of this project is not "I implemented Raft". It is "I can show, with
evidence, what stays correct under crashes, partitions, clock skew and message
chaos". That evidence does not exist yet. As of **Tier 1, M1**, only the
deterministic substrate is in place: injected time/transport/storage, seeded
message chaos, fault-injecting storage, the Raft log and RPC framing, and a
replayable trace recorder.

| Artifact | Status |
|---|---|
| Chaos runs / seeds completed | 0 (runner arrives in M7) |
| Mutation-detection table | empty (suite arrives in M7) |
| Linearizability checker | not built (M7) |
| Isolation checker | not built (M14) |
| TLA+ model-checking results | not run (M15) |
| Measured benchmarks | none |
| Replay viewer | not built (M16) |

Nothing in this README is a measured result. Numbers and failure stories will
be added only when they have actually been produced by a run in this
repository.

## Milestones

| Tier | Milestone | State |
|---|---|---|
| 1 | M1 Foundations | done |
| 1 | M2 Election + pre-vote | planned |
| 1 | M3 Log replication | planned |
| 1 | M4 Persistence / crash recovery | planned |
| 1 | M5 Snapshots, membership, transfer | planned |
| 1 | M6 Client semantics / linearizable reads | planned |
| 1 | M7 Chaos engine + linearizability checker | planned |
| 2–5 | M8–M17 | planned |

Work proceeds one milestone at a time; each one ends with all tests run and a
short writeup of the rules implemented, the tradeoffs and open questions.

## What exists now

```
src/quorum/runtime/   Clock, Transport, StorageIO, StateMachine, TraceSink
src/quorum/raft/      messages.py (RPCs + framing), log.py (1-indexed log)
sim/                  network.py (seeded chaos), trace.py (recorder)
docs/DESIGN.md        layering, determinism rules, persistence ordering rules
tests/                log, framing, clock, network, storage, trace tests
```

The core is standard library only. `pytest`, `hypothesis` and `mypy` are
dev-only, and all code is checked with `mypy --strict`.

## Quick start

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'

pytest -q
mypy --strict
```

To replay a trace by seed later, the chaos runner will expose `--seed N`; the
trace format it writes is described in `docs/DESIGN.md`.

## Guarantees and non-guarantees

Guaranteed in M1: byte-identical traces for identical seeds and scripts; no
core component reads the wall clock, uses unseeded randomness, or touches a
real disk or socket; durability is explicit and testable.

Not guaranteed in M1: anything about consensus, replication, reads, writes,
transactions or availability. There is no database yet.

## License

MIT. See `LICENSE`.
