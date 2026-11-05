"""The same seed must replay identically even when Python's hash salt changes."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

DEMO = Path(__file__).resolve().parents[1] / "demo" / "m1_determinism.py"


def _trace(hash_seed: str) -> str:
    env = dict(os.environ, PYTHONHASHSEED=hash_seed)
    result = subprocess.run(
        [sys.executable, str(DEMO), "--json"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def test_trace_is_identical_across_hash_seeds() -> None:
    first = _trace("1")
    second = _trace("98765")
    assert first == second
    parsed = json.loads(first)
    assert parsed["meta"]["seed"] == 9137
    assert parsed["meta"]["event_count"] == len(parsed["events"])
