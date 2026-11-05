"""Seeded, deterministic in-process network.

Deliveries live in a heap keyed by ``(deliver_at, sequence)``; the sequence
breaks ties, so no ordering decision depends on hashing or set iteration. Every
drop is an explicit trace event with a reason, which is what lets the M7 chaos
runner minimize a failing trace.

Each node interacts only through a ``SimTransport``, a per-node view satisfying
the core's ``Transport`` protocol. Nodes never see the queue, the RNG or time.
"""

from __future__ import annotations

import heapq
import random
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

from quorum.raft.messages import frame_label
from quorum.runtime.clock import MutableClock
from quorum.runtime.trace import NullSink, TraceSink
from quorum.runtime.transport import NodeId, Transport

MessageHandler = Callable[[NodeId, bytes], None]

MAX_MESSAGE_COPIES: int = 3
DEFAULT_MAX_EVENTS: int = 10_000


@dataclass(frozen=True, slots=True)
class LinkPolicy:
    """Per-direction link behaviour. Probabilities are in [0, 1]."""

    drop_probability: float = 0.0
    duplicate_probability: float = 0.0
    min_delay: int = 1
    max_delay: int = 1
    reorder_probability: float = 0.0
    reorder_extra_delay: int = 10
    cut: bool = False

    def __post_init__(self) -> None:
        for name in ("drop_probability", "duplicate_probability", "reorder_probability"):
            value = float(getattr(self, name))
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.min_delay < 0 or self.max_delay < self.min_delay:
            raise ValueError("delay range must be non-negative and ordered")
        if self.reorder_extra_delay < 1:
            raise ValueError("reorder_extra_delay must be positive")


@dataclass(frozen=True, slots=True)
class _Delivery:
    message_id: str
    src: NodeId
    dst: NodeId
    frame: bytes
    copy: int


class SimNetwork:
    def __init__(
        self,
        clock: MutableClock,
        *,
        seed: int,
        sink: TraceSink | None = None,
        default: LinkPolicy | None = None,
    ) -> None:
        self._clock = clock
        self._seed = seed
        self._rng = random.Random(seed)
        self._sink: TraceSink = sink if sink is not None else NullSink()
        self._default = default if default is not None else LinkPolicy()
        self._links: dict[tuple[NodeId, NodeId], LinkPolicy] = {}
        self._handlers: dict[NodeId, MessageHandler] = {}
        self._alive: dict[NodeId, bool] = {}
        self._groups: dict[NodeId, int] = {}
        self._queue: list[tuple[int, int, _Delivery]] = []
        self._sequence = 0
        self._message_counter = 0
        self._sent = 0
        self._received = 0
        self._dropped = 0

    @property
    def seed(self) -> int:
        return self._seed

    @property
    def sent(self) -> int:
        return self._sent

    @property
    def received(self) -> int:
        return self._received

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def pending(self) -> int:
        return len(self._queue)

    def register(self, node: NodeId, handler: MessageHandler) -> None:
        self._handlers[node] = handler
        self._alive.setdefault(node, True)

    def transport(self, node: NodeId) -> Transport:
        return SimTransport(self, node)

    def set_link(self, src: NodeId, dst: NodeId, policy: LinkPolicy) -> None:
        self._links[(src, dst)] = policy
        self._sink.record(
            "config",
            node=src,
            peer=dst,
            change="link",
            drop_probability=policy.drop_probability,
            duplicate_probability=policy.duplicate_probability,
            min_delay=policy.min_delay,
            max_delay=policy.max_delay,
            reorder_probability=policy.reorder_probability,
            cut=policy.cut,
        )

    def set_partition(self, groups: Sequence[Iterable[NodeId]]) -> None:
        """Only nodes in the same group can talk; unlisted nodes are isolated."""
        listed = [sorted(group) for group in groups]
        mapping: dict[NodeId, int] = {}
        for index, group in enumerate(listed):
            for node in group:
                if node in mapping:
                    raise ValueError(f"node {node!r} appears in two partition groups")
                mapping[node] = index
        self._groups = mapping
        self._sink.record("config", change="partition", groups=listed)

    def clear_partition(self) -> None:
        self._groups = {}
        self._sink.record("config", change="partition_cleared")

    def set_alive(self, node: NodeId, alive: bool) -> None:
        self._alive[node] = alive
        self._sink.record("crash" if not alive else "restart", node=node)

    def crash(self, node: NodeId) -> None:
        self.set_alive(node, False)

    def restart(self, node: NodeId) -> None:
        self.set_alive(node, True)

    def send(self, src: NodeId, dst: NodeId, frame: bytes) -> None:
        self._message_counter += 1
        message_id = f"{src}:{self._message_counter}"
        self._sent += 1
        label = frame_label(frame)
        self._record(
            "send",
            node=src,
            peer=dst,
            message_id=message_id,
            message_type=label,
            size=len(frame),
        )
        policy = self._links.get((src, dst), self._default)
        reason = self._drop_reason(src, dst, policy)
        if reason is not None:
            self._drop(message_id, src, dst, reason, copy=0, label=label)
            return
        delay = self._rng.randint(policy.min_delay, policy.max_delay)
        if policy.reorder_probability > 0.0 and self._rng.random() < policy.reorder_probability:
            delay += self._rng.randint(1, policy.reorder_extra_delay)
        copies = 1
        while (
            copies < MAX_MESSAGE_COPIES
            and policy.duplicate_probability > 0.0
            and self._rng.random() < policy.duplicate_probability
        ):
            copies += 1
        for copy in range(copies):
            self._schedule(
                self._clock.now() + delay + copy,
                _Delivery(message_id, src, dst, frame, copy),
            )

    def deliver_next(self) -> bool:
        if not self._queue:
            return False
        at, _, delivery = heapq.heappop(self._queue)
        if at > self._clock.now():
            self._clock.advance(at - self._clock.now())
        if not self._alive.get(delivery.dst, True):
            self._drop(
                delivery.message_id,
                delivery.src,
                delivery.dst,
                "receiver_down_after_send",
                copy=delivery.copy,
                label=frame_label(delivery.frame),
            )
            return True
        self._received += 1
        self._record(
            "receive",
            node=delivery.dst,
            peer=delivery.src,
            message_id=delivery.message_id,
            message_type=frame_label(delivery.frame),
            copy=delivery.copy,
            size=len(delivery.frame),
        )
        handler = self._handlers.get(delivery.dst)
        if handler is not None:
            handler(delivery.src, delivery.frame)
        return True

    def run_until_idle(self, max_events: int = DEFAULT_MAX_EVENTS) -> int:
        """Drain the queue; handlers may enqueue more work up to ``max_events``."""
        delivered = 0
        while self._queue:
            if delivered >= max_events:
                raise RuntimeError(f"network did not settle after {max_events} deliveries")
            self.deliver_next()
            delivered += 1
        return delivered

    def run_until(self, time: int) -> int:
        """Deliver everything scheduled up to ``time``, then move the clock there."""
        if time < self._clock.now():
            raise ValueError("cannot run into the past")
        delivered = 0
        while self._queue and self._queue[0][0] <= time:
            self.deliver_next()
            delivered += 1
        if time > self._clock.now():
            self._clock.advance(time - self._clock.now())
        return delivered

    def _drop_reason(self, src: NodeId, dst: NodeId, policy: LinkPolicy) -> str | None:
        if not self._alive.get(src, True):
            return "sender_down"
        if not self._alive.get(dst, True):
            return "receiver_down"
        if self._groups:
            src_group = self._groups.get(src)
            dst_group = self._groups.get(dst)
            if src_group is None or dst_group is None or src_group != dst_group:
                return "partition"
        if policy.cut:
            return "link_cut"
        if policy.drop_probability > 0.0 and self._rng.random() < policy.drop_probability:
            return "random_drop"
        return None

    def _drop(
        self,
        message_id: str,
        src: NodeId,
        dst: NodeId,
        reason: str,
        *,
        copy: int,
        label: str | None,
    ) -> None:
        self._dropped += 1
        self._record(
            "drop",
            node=dst,
            peer=src,
            message_id=message_id,
            message_type=label,
            copy=copy,
            reason=reason,
        )

    def _schedule(self, at: int, delivery: _Delivery) -> None:
        self._sequence += 1
        heapq.heappush(self._queue, (at, self._sequence, delivery))

    def _record(
        self,
        kind: str,
        *,
        node: NodeId | None = None,
        peer: NodeId | None = None,
        **details: object,
    ) -> None:
        self._sink.record(kind, node=node, peer=peer, **details)


class SimTransport:
    """A single node's view of the simulated network."""

    def __init__(self, network: SimNetwork, node: NodeId) -> None:
        self._network = network
        self._node = node

    @property
    def node(self) -> NodeId:
        return self._node

    def send(self, destination: NodeId, frame: bytes) -> None:
        self._network.send(self._node, destination, frame)
