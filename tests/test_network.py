from __future__ import annotations

from collections.abc import Callable

import pytest

from quorum.raft.messages import RequestVote, decode_message, encode_message
from quorum.runtime.clock import FakeClock
from quorum.runtime.transport import Transport
from sim.network import LinkPolicy, SimNetwork
from sim.trace import Trace, TraceRecorder

NODES = ("a", "b", "c")
PAIRS = (("a", "b"), ("b", "c"), ("c", "a"), ("a", "c"))
CHAOS = LinkPolicy(
    drop_probability=0.3,
    duplicate_probability=0.2,
    min_delay=1,
    max_delay=5,
    reorder_probability=0.4,
    reorder_extra_delay=10,
)


def _frame(term: int) -> bytes:
    return encode_message(
        RequestVote(term=term, candidate_id="a", last_log_index=0, last_log_term=0)
    )


def _term_handler(
    deliveries: list[tuple[str, int, int]], node: str
) -> Callable[[str, bytes], None]:
    def on_message(sender: str, frame: bytes) -> None:
        deliveries.append((node, len(frame), decode_message(frame).term))

    return on_message


def _node_handler(received: list[str], node: str) -> Callable[[str, bytes], None]:
    def on_message(sender: str, frame: bytes) -> None:
        received.append(node)

    return on_message


def _term_list_handler(terms: list[int]) -> Callable[[str, bytes], None]:
    def on_message(sender: str, frame: bytes) -> None:
        terms.append(decode_message(frame).term)

    return on_message


def _ignore(sender: str, frame: bytes) -> None:
    return None


def _setup(
    seed: int, policy: LinkPolicy | None = None
) -> tuple[FakeClock, TraceRecorder, SimNetwork]:
    clock = FakeClock()
    recorder = TraceRecorder(clock, seed=seed, config={"nodes": list(NODES)})
    network = SimNetwork(clock, seed=seed, sink=recorder, default=policy)
    return clock, recorder, network


def _run(seed: int, policy: LinkPolicy | None = None) -> tuple[Trace, list[tuple[str, int, int]]]:
    clock, recorder, network = _setup(seed, policy)
    deliveries: list[tuple[str, int, int]] = []
    for node in NODES:
        network.register(node, _term_handler(deliveries, node))
    for round_number in range(8):
        for index, (src, dst) in enumerate(PAIRS):
            network.send(src, dst, _frame(round_number * 10 + index))
        network.run_until_idle()
        clock.advance(2)
    return recorder.snapshot(), deliveries


def test_same_seed_produces_a_byte_identical_trace() -> None:
    first, first_deliveries = _run(1234, CHAOS)
    second, second_deliveries = _run(1234, CHAOS)
    assert first.to_json() == second.to_json()
    assert first_deliveries == second_deliveries
    assert len(first_deliveries) > 0


def test_different_seeds_produce_different_traces() -> None:
    first, _ = _run(101, CHAOS)
    second, _ = _run(202, CHAOS)
    assert first.to_json() != second.to_json()


def test_every_send_has_a_terminal_outcome_and_time_never_goes_backwards() -> None:
    trace, _ = _run(101, CHAOS)
    sends = {
        str(event.details["message_id"]): event
        for event in trace.events
        if event.kind == "send"
    }
    outcomes = [event for event in trace.events if event.kind in ("receive", "drop")]
    assert len(sends) == 8 * len(PAIRS)
    assert {str(event.details["message_id"]) for event in outcomes} == set(sends)
    for event in outcomes:
        send = sends[str(event.details["message_id"])]
        assert event.time >= send.time
    assert [event.seq for event in trace.events] == list(range(len(trace.events)))
    times = [event.time for event in trace.events]
    assert times == sorted(times)


def test_trace_labels_message_types() -> None:
    trace, _ = _run(3, LinkPolicy())
    sends = [event for event in trace.events if event.kind == "send"]
    assert all(event.details["message_type"] == "RequestVote" for event in sends)


def test_link_cut_is_asymmetric() -> None:
    _, recorder, network = _setup(0)
    received: list[str] = []
    for node in ("a", "b"):
        network.register(node, _node_handler(received, node))
    network.set_link("a", "b", LinkPolicy(cut=True))
    network.send("a", "b", _frame(1))
    network.send("b", "a", _frame(2))
    network.run_until_idle()
    assert received == ["a"]
    drops = [event for event in recorder.events if event.kind == "drop"]
    assert len(drops) == 1
    assert drops[0].details["reason"] == "link_cut"


def test_group_partition_only_drops_cross_group_traffic() -> None:
    _, recorder, network = _setup(0)
    received: list[str] = []
    for node in NODES:
        network.register(node, _node_handler(received, node))
    network.set_partition([{"a", "b"}, {"c"}])
    network.send("a", "b", _frame(1))
    network.send("a", "c", _frame(2))
    network.send("c", "a", _frame(3))
    network.run_until_idle()
    assert received == ["b"]
    drops = [event for event in recorder.events if event.kind == "drop"]
    assert [event.details["reason"] for event in drops] == ["partition", "partition"]
    network.clear_partition()
    network.send("a", "c", _frame(4))
    network.run_until_idle()
    assert received == ["b", "c"]


def test_partition_groups_must_be_disjoint() -> None:
    _, _, network = _setup(0)
    with pytest.raises(ValueError):
        network.set_partition([{"a", "b"}, {"b", "c"}])


def test_crashed_node_neither_sends_nor_receives_until_restarted() -> None:
    _, recorder, network = _setup(0)
    received: list[str] = []
    for node in ("a", "b"):
        network.register(node, _node_handler(received, node))
    network.crash("b")
    network.send("a", "b", _frame(1))
    network.send("b", "a", _frame(2))
    network.restart("b")
    network.send("a", "b", _frame(3))
    network.run_until_idle()
    assert received == ["b"]
    reasons = [
        event.details["reason"]
        for event in recorder.events
        if event.kind == "drop"
    ]
    assert reasons == ["receiver_down", "sender_down"]
    assert [event.kind for event in recorder.events].count("crash") == 1
    assert [event.kind for event in recorder.events].count("restart") == 1


def test_crash_after_send_drops_the_scheduled_delivery() -> None:
    _, recorder, network = _setup(0, LinkPolicy(min_delay=5, max_delay=5))
    network.register("b", _ignore)
    network.send("a", "b", _frame(1))
    network.crash("b")
    network.run_until_idle()
    drops = [event for event in recorder.events if event.kind == "drop"]
    assert [event.details["reason"] for event in drops] == ["receiver_down_after_send"]


def test_run_until_respects_the_time_boundary() -> None:
    clock, _, network = _setup(0, LinkPolicy(min_delay=10, max_delay=10))
    network.register("b", _ignore)
    network.send("a", "b", _frame(1))
    assert network.run_until(5) == 0
    assert clock.now() == 5
    assert network.pending == 1
    assert network.run_until(10) == 1
    assert clock.now() == 10
    assert network.pending == 0
    with pytest.raises(ValueError):
        network.run_until(9)


def test_duplication_is_capped_and_replayable() -> None:
    policy = LinkPolicy(duplicate_probability=1.0, min_delay=1, max_delay=1)
    trace, deliveries = _run(77, policy)
    sends = [event for event in trace.events if event.kind == "send"]
    receives = [event for event in trace.events if event.kind == "receive"]
    assert len(receives) == 3 * len(sends)
    assert len(deliveries) == len(receives)
    assert trace == _run(77, policy)[0]


def test_reordering_can_deliver_a_later_send_first() -> None:
    clock, recorder, network = _setup(
        5,
        LinkPolicy(
            min_delay=1,
            max_delay=1,
            reorder_probability=1.0,
            reorder_extra_delay=20,
        ),
    )
    order: list[int] = []
    network.register("b", _term_list_handler(order))
    network.send("a", "b", _frame(1))
    network.send("a", "b", _frame(2))
    network.run_until_idle()
    assert sorted(order) == [1, 2]
    assert order == [2, 1]
    assert clock.now() > 1
    assert len(recorder.events) == 4


def test_sim_transport_satisfies_the_core_protocol() -> None:
    _, _, network = _setup(0)
    received: list[int] = []
    network.register("b", _term_list_handler(received))
    transport: Transport = network.transport("a")
    assert isinstance(transport, Transport)
    transport.send("b", _frame(9))
    network.run_until_idle()
    assert received == [9]


def test_link_policy_rejects_invalid_values() -> None:
    with pytest.raises(ValueError):
        LinkPolicy(drop_probability=1.1)
    with pytest.raises(ValueError):
        LinkPolicy(min_delay=5, max_delay=2)
    with pytest.raises(ValueError):
        LinkPolicy(min_delay=-1)


def test_config_changes_are_recorded() -> None:
    _, recorder, network = _setup(0)
    network.set_partition([{"a"}, {"b"}])
    network.set_link("a", "b", LinkPolicy(cut=True))
    changes = [event for event in recorder.events if event.kind == "config"]
    assert [event.details["change"] for event in changes] == ["partition", "link"]
