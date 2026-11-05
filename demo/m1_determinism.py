"""Determinism demo for M1.

Runs the same seeded chaos script twice and compares the exported traces. With
``--json`` it emits the raw trace instead, which is what the hash-seed test in
tests/test_determinism.py compares across interpreter processes.
"""

from __future__ import annotations

import hashlib
import sys
from collections import Counter
from pathlib import Path

# sim/ is top-level in this repository, so make the checkout importable even
# when this file is run directly rather than as an installed module.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from quorum.raft.messages import RequestVote, encode_message  # noqa: E402
from quorum.runtime.clock import FakeClock  # noqa: E402
from sim.network import LinkPolicy, SimNetwork  # noqa: E402
from sim.trace import Trace, TraceRecorder  # noqa: E402

SEED = 9137
NODES = ("a", "b", "c")
PAIRS = (("a", "b"), ("b", "c"), ("c", "a"), ("a", "c"))


def _ignore(sender: str, frame: bytes) -> None:
    return None


def build_trace() -> str:
    clock = FakeClock()
    recorder = TraceRecorder(clock, seed=SEED, config={"nodes": list(NODES)})
    network = SimNetwork(
        clock,
        seed=SEED,
        sink=recorder,
        default=LinkPolicy(
            drop_probability=0.25,
            duplicate_probability=0.2,
            min_delay=1,
            max_delay=6,
            reorder_probability=0.5,
            reorder_extra_delay=12,
        ),
    )
    for node in NODES:
        network.register(node, _ignore)
    for round_number in range(12):
        for index, (src, dst) in enumerate(PAIRS):
            network.send(
                src,
                dst,
                encode_message(
                    RequestVote(
                        term=round_number,
                        candidate_id=src,
                        last_log_index=index,
                        last_log_term=round_number,
                    )
                ),
            )
        network.run_until_idle()
        clock.advance(3)
    return recorder.snapshot().to_json()


def _counts(payload: str) -> Counter[str]:
    return Counter(event.kind for event in Trace.from_json(payload).events)


def main() -> None:
    first = build_trace()
    if "--json" in sys.argv[1:]:
        sys.stdout.write(first)
        return
    second = build_trace()
    digest = hashlib.sha256(first.encode("utf-8")).hexdigest()
    second_digest = hashlib.sha256(second.encode("utf-8")).hexdigest()
    counts = _counts(first)
    print(f"seed                {SEED}")
    print(f"send/receive/drop   {counts['send']} / {counts['receive']} / {counts['drop']}")
    print(f"events              {sum(counts.values())}")
    print(f"trace sha256        {digest}")
    print(f"second run sha256   {second_digest}")
    print(f"identical           {'yes' if digest == second_digest else 'NO'}")
    if digest != second_digest:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
