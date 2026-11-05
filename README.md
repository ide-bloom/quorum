# Quorum

[![CI](https://github.com/ide-bloom/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/ide-bloom/quorum/actions/workflows/ci.yml)

Building a Raft-based distributed key-value database in Python, with a
deterministic simulator and fault-injection testing. **Status: Milestone 1 of
17 (foundations).** There is no database yet — M1 is the substrate the later
layers build on.

## Why this exists

Most Raft implementations are tested by running a cluster and hoping the bugs
show up. This one starts from the opposite end: time, randomness, network and
disk are injected, so every run replays exactly from a seed and a failure can
be reproduced rather than described. That makes the first milestone plumbing
rather than consensus — not much to look at, but it is what makes later safety
claims checkable. The goal is that every "this stays correct under faults"
statement eventually comes with a trace and a checker behind it.

## Verification roadmap

The point of this project is not "I implemented Raft". It is "I can show, with
evidence, what stays correct under crashes, partitions, clock skew and message
chaos". That evidence does not exist yet. As of **Tier 1, M1**, only the
deterministic substrate is in place: injected time/transport/storage, seeded
message chaos, fault-injecting storage, the Raft log and RPC framing, and a
replayable trace recorder. The table is a plan, not a list of results.

| Artifact | Status |
|---|---|
| Chaos runs / seeds completed | 0 (runner arrives in M7) |
| Mutation-detection table | empty (suite arrives in M7) |
| Linearizability checker | not built (M7) |
| Isolation checker | not built (M14) |
| TLA+ model-checking results | not run (M15) |
| Measured benchmarks | none |
| Replay viewer | not built (M16) |

Nothing in this README is a measured result except the demo output below.
Numbers, coverage figures and failure stories will be added only when they have
actually been produced by a run in this repository.

## Milestones

Tier 1 is in progress; Tiers 2–5 are planned, not promises.

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
demo/                 m1_determinism.py (seeded replay demo)
.github/workflows/    ci.yml (pytest, coverage, mypy --strict)
docs/DESIGN.md        layering, determinism rules, persistence ordering rules
tests/                log, framing, clock, network, storage, trace, determinism
```

The core is standard library only. `pytest`, `pytest-cov`, `hypothesis` and
`mypy` are dev-only, and all code is checked with `mypy --strict`.

## Quick start

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'

pytest -q
mypy --strict
python demo/m1_determinism.py
```

CI runs the same tests, coverage and type check on every push and pull request.

## Determinism demo

`demo/m1_determinism.py` runs one seeded chaos script twice and compares the
exported traces. Actual output from this checkout:

```text
seed                9137
send/receive/drop   48 / 41 / 15
events              104
trace sha256        8e875e7bc00e31b3d083157053599728302e7d5cbea55247c01450312a5fb975
second run sha256   8e875e7bc00e31b3d083157053599728302e7d5cbea55247c01450312a5fb975
identical           yes
```

`tests/test_determinism.py` repeats that comparison across two interpreter
processes with different `PYTHONHASHSEED` values. The full trace is available
with `python demo/m1_determinism.py --json`; `docs/DESIGN.md` documents the
format, and M7 will add `--seed` replay.

## Guarantees and non-guarantees

Guaranteed in M1: byte-identical traces for identical seeds and scripts; no
core component reads the wall clock, uses unseeded randomness, or touches a
real disk or socket; durability is explicit and testable.

Not guaranteed in M1: anything about consensus, replication, reads, writes,
transactions or availability. There is no database yet.

## License

MIT. See `LICENSE`.
