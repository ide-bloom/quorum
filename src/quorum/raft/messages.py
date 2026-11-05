"""Raft RPC dataclasses and their wire format. # RAFT §5.2, §5.3, §7, thesis §6.2

One frame is a 4-byte big-endian length followed by a compact UTF-8 JSON
object; the ``type`` field selects the dataclass. Snapshot chunks are base64
encoded so the wire stays plain JSON. Unknown or malformed frames raise
``ValueError`` rather than being silently skipped -- a corrupted frame is a
fault, not an empty message.

``pre_vote`` is carried from the start so M2 does not have to change the wire
format; it is unused in M1.
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Final, TypeAlias

from quorum.raft.log import LogEntry
from quorum.runtime.transport import NodeId

FRAME_HEADER_BYTES: Final = 4
MAX_FRAME_BYTES: Final = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class RequestVote:
    term: int
    candidate_id: NodeId
    last_log_index: int
    last_log_term: int
    pre_vote: bool = False


@dataclass(frozen=True, slots=True)
class RequestVoteReply:
    term: int
    vote_granted: bool
    voter_id: NodeId
    pre_vote: bool = False


@dataclass(frozen=True, slots=True)
class AppendEntries:
    term: int
    leader_id: NodeId
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True, slots=True)
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int
    conflict_term: int = 0
    conflict_index: int = 0


@dataclass(frozen=True, slots=True)
class InstallSnapshot:
    term: int
    leader_id: NodeId
    last_included_index: int
    last_included_term: int
    offset: int
    data: bytes
    done: bool


@dataclass(frozen=True, slots=True)
class InstallSnapshotReply:
    term: int
    last_included_index: int


@dataclass(frozen=True, slots=True)
class TimeoutNow:
    term: int
    leader_id: NodeId
    target_id: NodeId


@dataclass(frozen=True, slots=True)
class TimeoutNowReply:
    term: int
    accepted: bool
    voter_id: NodeId


Message: TypeAlias = (
    RequestVote
    | RequestVoteReply
    | AppendEntries
    | AppendEntriesReply
    | InstallSnapshot
    | InstallSnapshotReply
    | TimeoutNow
    | TimeoutNowReply
)


def _to_wire(value: object) -> object:
    if isinstance(value, LogEntry):
        return {"term": value.term, "command": value.command}
    if isinstance(value, bytes):
        return {"$b64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (tuple, list)):
        return [_to_wire(item) for item in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    raise TypeError(f"cannot encode {type(value).__name__}")


def encode_message(message: Message) -> bytes:
    body: dict[str, object] = {"type": type(message).__name__}
    for field in dataclasses.fields(message):
        body[field.name] = _to_wire(getattr(message, field.name))
    payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_FRAME_BYTES:
        raise ValueError("frame exceeds MAX_FRAME_BYTES")
    return len(payload).to_bytes(FRAME_HEADER_BYTES, "big") + payload


def iter_frames(stream: bytes) -> Iterator[bytes]:
    """Split concatenated frames, rejecting a truncated tail."""
    offset = 0
    while offset < len(stream):
        if len(stream) - offset < FRAME_HEADER_BYTES:
            raise ValueError("truncated frame header")
        size = int.from_bytes(stream[offset : offset + FRAME_HEADER_BYTES], "big")
        if size > MAX_FRAME_BYTES:
            raise ValueError("frame exceeds MAX_FRAME_BYTES")
        end = offset + FRAME_HEADER_BYTES + size
        if end > len(stream):
            raise ValueError("truncated frame body")
        yield stream[offset:end]
        offset = end


def decode_message(frame: bytes) -> Message:
    if len(frame) < FRAME_HEADER_BYTES:
        raise ValueError("truncated frame header")
    size = int.from_bytes(frame[:FRAME_HEADER_BYTES], "big")
    if size > MAX_FRAME_BYTES or len(frame) != FRAME_HEADER_BYTES + size:
        raise ValueError("frame length does not match its header")
    try:
        raw = json.loads(frame[FRAME_HEADER_BYTES:].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("frame is not valid JSON") from exc
    if not isinstance(raw, dict):
        raise ValueError("frame body must be an object")
    name = raw.get("type")
    if not isinstance(name, str) or name not in _DECODERS:
        raise ValueError(f"unknown message type {name!r}")
    return _DECODERS[name](raw)


def decode_stream(stream: bytes) -> tuple[Message, ...]:
    return tuple(decode_message(frame) for frame in iter_frames(stream))


def frame_label(frame: bytes) -> str | None:
    """Best-effort type name for trace labels; never raises on junk payloads."""
    try:
        return type(decode_message(frame)).__name__
    except ValueError:
        return None


def _field(raw: dict[str, Any], key: str) -> Any:
    if key not in raw:
        raise ValueError(f"missing field {key!r}")
    return raw[key]


def _int(raw: dict[str, Any], key: str, default: int | None = None) -> int:
    if key not in raw:
        if default is None:
            raise ValueError(f"missing field {key!r}")
        return default
    value = raw[key]
    if type(value) is not int:
        raise ValueError(f"field {key!r} must be an int")
    return value


def _str(raw: dict[str, Any], key: str) -> str:
    value = _field(raw, key)
    if not isinstance(value, str):
        raise ValueError(f"field {key!r} must be a str")
    return value


def _bool(raw: dict[str, Any], key: str, default: bool = False) -> bool:
    value = raw.get(key, default)
    if type(value) is not bool:
        raise ValueError(f"field {key!r} must be a bool")
    return value


def _b64(raw: dict[str, Any], key: str) -> bytes:
    value = _field(raw, key)
    if not isinstance(value, dict) or not isinstance(value.get("$b64"), str):
        raise ValueError(f"field {key!r} must be a base64 wrapper")
    try:
        return base64.b64decode(value["$b64"], validate=True)
    except binascii.Error as exc:
        raise ValueError(f"field {key!r} is not valid base64") from exc


def _entries(raw: dict[str, Any], key: str) -> tuple[LogEntry, ...]:
    value = _field(raw, key)
    if not isinstance(value, list):
        raise ValueError(f"field {key!r} must be a list")
    result: list[LogEntry] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError(f"field {key!r} contains a non-object entry")
        result.append(LogEntry(term=_int(item, "term"), command=_str(item, "command")))
    return tuple(result)


def _decode_request_vote(raw: dict[str, Any]) -> RequestVote:
    return RequestVote(
        term=_int(raw, "term"),
        candidate_id=_str(raw, "candidate_id"),
        last_log_index=_int(raw, "last_log_index"),
        last_log_term=_int(raw, "last_log_term"),
        pre_vote=_bool(raw, "pre_vote"),
    )


def _decode_request_vote_reply(raw: dict[str, Any]) -> RequestVoteReply:
    return RequestVoteReply(
        term=_int(raw, "term"),
        vote_granted=_bool(raw, "vote_granted"),
        voter_id=_str(raw, "voter_id"),
        pre_vote=_bool(raw, "pre_vote"),
    )


def _decode_append_entries(raw: dict[str, Any]) -> AppendEntries:
    return AppendEntries(
        term=_int(raw, "term"),
        leader_id=_str(raw, "leader_id"),
        prev_log_index=_int(raw, "prev_log_index"),
        prev_log_term=_int(raw, "prev_log_term"),
        entries=_entries(raw, "entries"),
        leader_commit=_int(raw, "leader_commit"),
    )


def _decode_append_entries_reply(raw: dict[str, Any]) -> AppendEntriesReply:
    return AppendEntriesReply(
        term=_int(raw, "term"),
        success=_bool(raw, "success"),
        match_index=_int(raw, "match_index"),
        conflict_term=_int(raw, "conflict_term", 0),
        conflict_index=_int(raw, "conflict_index", 0),
    )


def _decode_install_snapshot(raw: dict[str, Any]) -> InstallSnapshot:
    return InstallSnapshot(
        term=_int(raw, "term"),
        leader_id=_str(raw, "leader_id"),
        last_included_index=_int(raw, "last_included_index"),
        last_included_term=_int(raw, "last_included_term"),
        offset=_int(raw, "offset"),
        data=_b64(raw, "data"),
        done=_bool(raw, "done"),
    )


def _decode_install_snapshot_reply(raw: dict[str, Any]) -> InstallSnapshotReply:
    return InstallSnapshotReply(
        term=_int(raw, "term"),
        last_included_index=_int(raw, "last_included_index"),
    )


def _decode_timeout_now(raw: dict[str, Any]) -> TimeoutNow:
    return TimeoutNow(
        term=_int(raw, "term"),
        leader_id=_str(raw, "leader_id"),
        target_id=_str(raw, "target_id"),
    )


def _decode_timeout_now_reply(raw: dict[str, Any]) -> TimeoutNowReply:
    return TimeoutNowReply(
        term=_int(raw, "term"),
        accepted=_bool(raw, "accepted"),
        voter_id=_str(raw, "voter_id"),
    )


_DECODERS: Final[dict[str, Callable[[dict[str, Any]], Message]]] = {
    "RequestVote": _decode_request_vote,
    "RequestVoteReply": _decode_request_vote_reply,
    "AppendEntries": _decode_append_entries,
    "AppendEntriesReply": _decode_append_entries_reply,
    "InstallSnapshot": _decode_install_snapshot,
    "InstallSnapshotReply": _decode_install_snapshot_reply,
    "TimeoutNow": _decode_timeout_now,
    "TimeoutNowReply": _decode_timeout_now_reply,
}
