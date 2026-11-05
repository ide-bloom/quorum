from __future__ import annotations

import json

import pytest

from quorum.raft.log import LogEntry
from quorum.raft.messages import (
    AppendEntries,
    AppendEntriesReply,
    InstallSnapshot,
    InstallSnapshotReply,
    Message,
    RequestVote,
    RequestVoteReply,
    TimeoutNow,
    TimeoutNowReply,
    decode_message,
    decode_stream,
    encode_message,
    frame_label,
    iter_frames,
)

SAMPLES: list[Message] = [
    RequestVote(term=3, candidate_id="n1", last_log_index=7, last_log_term=2),
    RequestVote(term=4, candidate_id="n2", last_log_index=0, last_log_term=0, pre_vote=True),
    RequestVoteReply(term=3, vote_granted=True, voter_id="n2"),
    RequestVoteReply(term=4, vote_granted=False, voter_id="n2", pre_vote=True),
    AppendEntries(
        term=3,
        leader_id="n1",
        prev_log_index=7,
        prev_log_term=2,
        entries=(LogEntry(term=3, command="set a=1"), LogEntry(term=3, command="set b=2")),
        leader_commit=6,
    ),
    AppendEntries(term=1, leader_id="n1", prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0),
    AppendEntriesReply(term=3, success=False, match_index=0, conflict_term=2, conflict_index=5),
    AppendEntriesReply(term=3, success=True, match_index=9),
    InstallSnapshot(
        term=4,
        leader_id="n1",
        last_included_index=10,
        last_included_term=3,
        offset=0,
        data=b"\x00\xffchunk",
        done=True,
    ),
    InstallSnapshotReply(term=4, last_included_index=10),
    TimeoutNow(term=5, leader_id="n1", target_id="n3"),
    TimeoutNowReply(term=5, accepted=True, voter_id="n3"),
]


def _raw_frame(body: dict[str, object]) -> bytes:
    payload = json.dumps(body).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


@pytest.mark.parametrize("message", SAMPLES)
def test_round_trip(message: Message) -> None:
    frame = encode_message(message)
    assert frame[:4] == len(frame[4:]).to_bytes(4, "big")
    assert decode_message(frame) == message


def test_concatenated_frames_stream_in_order() -> None:
    stream = b"".join(encode_message(message) for message in SAMPLES)
    assert list(iter_frames(stream)) == [encode_message(message) for message in SAMPLES]
    assert decode_stream(stream) == tuple(SAMPLES)


def test_default_fields_survive_a_missing_wire_field() -> None:
    body = json.loads(encode_message(SAMPLES[7])[4:].decode("utf-8"))
    body.pop("conflict_term")
    body.pop("conflict_index")
    decoded = decode_message(_raw_frame(body))
    assert decoded == AppendEntriesReply(term=3, success=True, match_index=9)


@pytest.mark.parametrize(
    "frame",
    [
        b"",
        b"\x00\x00",
        b"\x00\x00\x00\x09abc",
        b"\x00\x00\x00\x02{}",
        b"\x00\x00\x00\x09not-json!",
    ],
)
def test_malformed_frames_are_rejected(frame: bytes) -> None:
    with pytest.raises(ValueError):
        decode_message(frame)


def test_unknown_type_and_wrong_field_type_are_rejected() -> None:
    with pytest.raises(ValueError):
        decode_message(_raw_frame({"type": "Nope", "term": 1}))
    with pytest.raises(ValueError):
        decode_message(
            _raw_frame(
                {
                    "type": "RequestVote",
                    "term": "three",
                    "candidate_id": "n1",
                    "last_log_index": 0,
                    "last_log_term": 0,
                    "pre_vote": False,
                }
            )
        )


def test_bad_base64_chunk_is_rejected() -> None:
    body = json.loads(encode_message(SAMPLES[8])[4:].decode("utf-8"))
    body["data"] = {"$b64": "!!!"}
    with pytest.raises(ValueError):
        decode_message(_raw_frame(body))


def test_truncated_tail_in_a_stream_is_rejected() -> None:
    stream = encode_message(SAMPLES[0]) + encode_message(SAMPLES[1])[:-2]
    with pytest.raises(ValueError):
        list(iter_frames(stream))


def test_frame_label_is_best_effort() -> None:
    assert frame_label(encode_message(SAMPLES[0])) == "RequestVote"
    assert frame_label(b"junk") is None
