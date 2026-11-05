"""Deterministic trace recording.

A trace is a flat, JSON-serializable list of events plus a header carrying the
format version, the seed and the run configuration. The chaos runner (M7)
minimizes these traces, ``--seed`` replay consumes them, and the M16 viewer
loads them directly. Nothing in the core imports this module; components only
see the ``TraceSink`` protocol.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from quorum.runtime.clock import Clock
from quorum.runtime.trace import TraceSink

TRACE_FORMAT: Final = 1


@dataclass(frozen=True, slots=True)
class TraceEvent:
    seq: int
    time: int
    kind: str
    node: str | None
    peer: str | None
    details: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "seq": self.seq,
            "time": self.time,
            "kind": self.kind,
            "node": self.node,
            "peer": self.peer,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> TraceEvent:
        details = raw.get("details", {})
        if not isinstance(details, dict):
            raise ValueError("event details must be an object")
        return cls(
            seq=_as_int(raw, "seq"),
            time=_as_int(raw, "time"),
            kind=_as_str(raw, "kind"),
            node=_optional_str(raw, "node"),
            peer=_optional_str(raw, "peer"),
            details=dict(details),
        )


@dataclass(frozen=True, slots=True)
class Trace:
    meta: dict[str, object]
    events: tuple[TraceEvent, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "meta": dict(self.meta),
            "events": [event.to_dict() for event in self.events],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, indent=2) + "\n"

    def write(self, path: str | Path) -> None:
        Path(path).write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def read(cls, path: str | Path) -> Trace:
        return cls.from_json(Path(path).read_text(encoding="utf-8"))

    @classmethod
    def from_json(cls, text: str) -> Trace:
        raw = json.loads(text)
        if not isinstance(raw, dict):
            raise ValueError("trace must be a JSON object")
        meta = raw.get("meta")
        events = raw.get("events")
        if not isinstance(meta, dict) or not isinstance(events, list):
            raise ValueError("trace needs 'meta' and 'events'")
        if meta.get("format") != TRACE_FORMAT:
            raise ValueError(f"unsupported trace format {meta.get('format')!r}")
        parsed = []
        for item in events:
            if not isinstance(item, dict):
                raise ValueError("each event must be an object")
            parsed.append(TraceEvent.from_dict(item))
        return cls(meta=dict(meta), events=tuple(parsed))


class TraceRecorder(TraceSink):
    """Concrete ``TraceSink``: stamps each event with logical time and a sequence."""

    def __init__(
        self,
        clock: Clock,
        *,
        seed: int,
        config: Mapping[str, object] | None = None,
    ) -> None:
        self._clock = clock
        self._seed = seed
        self._config: dict[str, object] = dict(config or {})
        _validate_json("config", self._config)
        self._events: list[TraceEvent] = []

    @property
    def events(self) -> tuple[TraceEvent, ...]:
        return tuple(self._events)

    def record(
        self,
        kind: str,
        *,
        node: str | None = None,
        peer: str | None = None,
        **details: object,
    ) -> None:
        payload = dict(details)
        _validate_json("event details", payload)
        self._events.append(
            TraceEvent(
                seq=len(self._events),
                time=self._clock.now(),
                kind=kind,
                node=node,
                peer=peer,
                details=payload,
            )
        )

    def snapshot(self) -> Trace:
        meta: dict[str, object] = {
            "format": TRACE_FORMAT,
            "seed": self._seed,
            "config": dict(self._config),
            "event_count": len(self._events),
        }
        return Trace(meta=meta, events=self.events)

    def to_json(self) -> str:
        return self.snapshot().to_json()

    def write(self, path: str | Path) -> None:
        self.snapshot().write(path)


def _validate_json(where: str, value: object) -> None:
    try:
        json.dumps(value, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"trace {where} must be JSON-serializable") from exc


def _as_int(raw: dict[str, Any], key: str) -> int:
    value = raw.get(key)
    if type(value) is not int:
        raise ValueError(f"{key!r} must be an int")
    return value


def _as_str(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{key!r} must be a str")
    return value


def _optional_str(raw: dict[str, Any], key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key!r} must be a str or null")
    return value
