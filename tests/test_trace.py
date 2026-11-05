from __future__ import annotations

import json
from pathlib import Path

import pytest

from quorum.runtime.clock import FakeClock
from quorum.runtime.trace import NullSink, TraceSink
from sim.trace import TRACE_FORMAT, Trace, TraceRecorder


def test_recorder_stamps_sequence_and_logical_time() -> None:
    clock = FakeClock()
    recorder = TraceRecorder(clock, seed=42, config={"nodes": 3})
    recorder.record("send", node="a", peer="b", message_id="a:1")
    clock.advance(5)
    recorder.record("drop", node="b", peer="a", reason="partition", message_id="a:1")
    events = recorder.events
    assert [event.seq for event in events] == [0, 1]
    assert [event.time for event in events] == [0, 5]
    assert [event.kind for event in events] == ["send", "drop"]
    assert events[0].node == "a" and events[0].peer == "b"
    assert events[1].details["reason"] == "partition"
    assert events[0].details["message_id"] == "a:1"


def test_snapshot_meta_carries_seed_and_config() -> None:
    recorder = TraceRecorder(FakeClock(), seed=17, config={"nodes": ["a", "b"]})
    recorder.record("timer", node="a")
    meta = recorder.snapshot().meta
    assert meta["format"] == TRACE_FORMAT
    assert meta["seed"] == 17
    assert meta["config"] == {"nodes": ["a", "b"]}
    assert meta["event_count"] == 1


def test_json_round_trip_and_file_round_trip(tmp_path: Path) -> None:
    clock = FakeClock()
    recorder = TraceRecorder(clock, seed=1, config={"policy": "lossy"})
    recorder.record("send", node="a", peer="b", size=12)
    clock.advance(3)
    recorder.record("receive", node="b", peer="a", size=12)
    original = recorder.snapshot()
    parsed = Trace.from_json(original.to_json())
    assert parsed == original
    path = tmp_path / "run.json"
    original.write(path)
    assert Trace.read(path) == original
    assert path.read_text(encoding="utf-8") == original.to_json()


def test_non_json_details_are_rejected_at_record_time() -> None:
    recorder = TraceRecorder(FakeClock(), seed=0)
    with pytest.raises(TypeError):
        recorder.record("send", payload={1, 2})
    with pytest.raises(TypeError):
        TraceRecorder(FakeClock(), seed=0, config={"bad": object()})


def test_from_json_rejects_bad_format_and_bad_events() -> None:
    recorder = TraceRecorder(FakeClock(), seed=0)
    recorder.record("crash", node="a")
    encoded = recorder.snapshot().to_json()

    bad_format = json.loads(encoded)
    bad_format["meta"]["format"] = 999
    with pytest.raises(ValueError):
        Trace.from_json(json.dumps(bad_format))

    bad_event = json.loads(encoded)
    bad_event["events"] = ["not-an-object"]
    with pytest.raises(ValueError):
        Trace.from_json(json.dumps(bad_event))

    bad_seq = json.loads(encoded)
    bad_seq["events"][0]["seq"] = "zero"
    with pytest.raises(ValueError):
        Trace.from_json(json.dumps(bad_seq))

    with pytest.raises(ValueError):
        Trace.from_json("[1, 2, 3]")


def test_null_sink_satisfies_the_trace_sink_protocol() -> None:
    sink: TraceSink = NullSink()
    assert isinstance(sink, TraceSink)
    sink.record("timer", node="a", ticks=1)
