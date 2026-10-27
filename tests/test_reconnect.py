"""
Broker restarts: own state goes back, foreign objects stay, offline writes behave as decided.
"""

# pytest injects fixtures by argument name
# pylint: disable=redefined-outer-name

import asyncio
import itertools
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterator

import aiomqtt
import pytest

from tests.broker import (
    BROKER_READS,
    FIRST_VALUE_META,
    DeadLinkProxy,
    MosquittoBroker,
    Send,
    ack_timeout,
    intercept_publish,
    internals,
    read_retained,
    restart_broker,
    wait_for_task_count,
    wait_or_dump,
    wait_retained,
    wait_until,
    window_of_controls,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import (
    BrokerDisconnectedError,
    Client,
    Control,
    ControlType,
    Message,
    broker_connection,
)
from wb_mqtt_lib import publisher as publisher_module
from wb_mqtt_lib import subscriber as subscriber_module

# The own device of the tests below as the broker keeps it
OWN_DEVICE = {
    "/devices/dev/meta": b'{"driver": "test-service"}',
    "/devices/dev/controls/temp/meta": FIRST_VALUE_META,
    "/devices/dev/controls/temp": b"21",
}
# Retained changes, the topic of each and what the broker keeps after it. A value of an own control returns
# before PUBACK, the meta and a direct message wait for it.
RETAINED_CHANGES: dict[str, tuple[Callable[[Client, Control], Awaitable[None]], str, bytes]] = {
    "set_value": (lambda client, temp: temp.set_value(2), "/devices/dev/controls/temp", b"2"),
    "update_meta": (
        lambda client, temp: temp.update_meta(units="V"),
        "/devices/dev/controls/temp/meta",
        b'{"type": "value", "readonly": false, "hidden": false, "units": "V", "order": 1}',
    ),
    "direct.publish": (
        lambda client, temp: client.direct.publish("/state/x", "1", retain=True),
        "/state/x",
        b"1",
    ),
}


@dataclass
class PublishLog:
    """
    What the clients of a test hand to paho under /devices, in that order. While slow, every publication
    holds its place in the window of the client a moment after PUBACK, so the queue behind it waits.
    """

    sent: list[tuple[str, Any]] = field(default_factory=list)
    slow: bool = False


@pytest.fixture
def publish_log(monkeypatch: pytest.MonkeyPatch) -> Iterator[PublishLog]:
    """
    Log of the publications of every client in the test, see PublishLog.
    """
    log = PublishLog()

    async def logged(send: Send, topic: str, payload: Any) -> None:
        if topic.startswith("/devices/"):
            log.sent.append((topic, payload))
        await send()
        if log.slow:
            await asyncio.sleep(0.2)

    intercept_publish(monkeypatch, logged)
    yield log


async def confirm_markers_late(send: Send, topic: str, _payload: Any) -> None:
    """
    The markers that end retained delivery reach the broker, their PUBACK comes too late.
    """
    await send()
    if topic.startswith("/wbretainhack/"):
        raise ack_timeout()


async def test_offline_own_state_goes_out_after_reconnect_and_events_fail(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    Own value and a control created without a link are kept and go out after reconnect. Commands and direct
    events fail at once.
    """
    await (await driver.create_device("relays")).create_control("K1", ControlType.SWITCH, value=False)
    device = await client.create_device("dev")
    temp = await device.create_control("temp", ControlType.VALUE, value=1)
    relay = await client.get_control("relays/K1")

    await asyncio.to_thread(broker.stop)
    await wait_until(lambda: not client.is_connected)
    await wait_or_dump(temp.set_value(2), timeout=1)
    await wait_or_dump(temp.set_value(3), timeout=1)
    humidity = await wait_or_dump(device.create_control("humidity", ControlType.VALUE, value=40), timeout=1)
    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(relay.set_value(True), timeout=1)
    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(client.direct.publish("/events/x", "1"), timeout=1)
    await asyncio.to_thread(broker.start)

    await wait_until(lambda: client.is_connected, timeout=10)
    await wait_retained(broker.port, "/devices/dev/controls/temp", {"/devices/dev/controls/temp": b"3"})
    humidity_meta = b'{"type": "value", "readonly": false, "hidden": false, "order": 2}'
    await wait_retained(
        broker.port,
        "/devices/dev/controls/humidity/#",
        {"/devices/dev/controls/humidity/meta": humidity_meta, "/devices/dev/controls/humidity": b"40"},
    )
    assert humidity.exists and humidity.value == 40
    await wait_until(lambda: relay.exists and relay.value is False, timeout=10)


async def test_foreign_objects_survive_restart_without_removed_events(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    A broker restart is not a removal, only erased meta is.
    """
    level = await (await driver.create_device("dev")).create_control("level", ControlType.VALUE, value=1)
    remote = await client.get_control("dev/level")
    removed = []
    remote.on_removed(removed.append)

    await restart_broker(broker, client, driver)
    await level.set_value(2)

    await wait_until(lambda: remote.value == 2, timeout=10)
    assert remote.exists and not removed


async def test_restart_storm_ends_in_consistent_state(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    Ten restarts while the driver keeps publishing.

    In the end the reader has the last value and nothing leaks.
    """
    level = await (await driver.create_device("dev")).create_control("level", ControlType.VALUE, value=0)
    remote = await client.get_control("dev/level")
    client_baseline, driver_baseline = internals(client), internals(driver)
    tasks_before = len(asyncio.all_tasks())
    stop = asyncio.Event()

    async def keep_publishing() -> int:
        value = 0
        while not stop.is_set():
            value += 1
            await level.set_value(value)
            await asyncio.sleep(0.005)
        return value

    publisher = asyncio.create_task(keep_publishing())
    for _ in range(10):
        await asyncio.to_thread(broker.restart)
        await asyncio.sleep(0.2)
    stop.set()
    last = await wait_or_dump(publisher, timeout=10)

    await wait_until(lambda: remote.value == last, timeout=15)
    await wait_until(lambda: internals(client) == client_baseline and internals(driver) == driver_baseline)
    # An attempt that broke right after the socket opened leaves the misc task of aiomqtt until paho closes
    # that socket by keepalive
    await wait_for_task_count(tasks_before, timeout=2 * broker_connection.KEEPALIVE_S)


async def test_command_reported_as_failed_is_not_delivered_after_reconnect(
    driver: Client, dead_link: tuple[DeadLinkProxy, Client]
) -> None:
    """
    A command into a link that is dead but not closed yet ends with BrokerDisconnectedError, and the driver
    does not get it after reconnect either.
    """
    relay = await (await driver.create_device("relays")).create_control("K1", ControlType.SWITCH, value=False)
    commands: list = []
    relay.on_command(lambda control, value: commands.append(value))
    proxy, client = dead_link
    remote = await client.get_control("relays/K1", timeout=5)

    proxy.dead = True
    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(remote.set_value(True), timeout=10)
    proxy.dead = False
    proxy.cut()
    await wait_until(lambda: not client.is_connected, timeout=5)
    await wait_until(lambda: client.is_connected, timeout=10)
    await asyncio.sleep(1)
    assert not commands

    await remote.set_value(False)

    await wait_until(lambda: commands == [False], timeout=5)


@pytest.mark.parametrize("call", BROKER_READS)
async def test_broker_reads_without_a_link_raise_and_work_after_reconnect(
    broker: MosquittoBroker, client: Client, call: str
) -> None:
    """
    Everything that reads the broker raises BrokerDisconnectedError without a link, leaves nothing behind
    and works after reconnect.
    """
    before = internals(client)
    await asyncio.to_thread(broker.stop)
    await wait_until(lambda: not client.is_connected)

    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(BROKER_READS[call](client), timeout=5)

    assert internals(client) == before
    await asyncio.to_thread(broker.start)
    await wait_until(lambda: client.is_connected, timeout=10)
    await wait_or_dump(BROKER_READS[call](client), timeout=5)


@pytest.mark.parametrize("offline", [False, True], ids=["online", "offline"])
async def test_invalid_publication_or_filter_is_refused_before_it_reaches_the_replay(
    broker: MosquittoBroker, client: Client, offline: bool
) -> None:
    """
    A topic, filter, QoS or payload the client or the broker would refuse is rejected at once,
    with or without a link. So is a shared subscription, the client does not support it.

    Kept for the replay or the resubscription after reconnect, it would break them every time. On a control
    character, a non-character or more than 200 slashes mosquitto drops the connection.
    """
    await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=21)
    before = internals(client)
    if offline:
        await asyncio.to_thread(broker.stop)
        await wait_until(lambda: not client.is_connected)
    received: list[Message] = []
    refused = [
        (client.direct.publish("a/+/b", "x", retain=True), ValueError),
        (client.direct.publish("", "x", retain=True), ValueError),
        (client.direct.publish("a/#", "x", retain=True), ValueError),
        (client.direct.publish("a/b", "x", retain=True, qos=3), ValueError),
        (client.direct.publish("a/b", "x", retain=True, qos=True), ValueError),
        (client.direct.publish("a/b", "x", retain=True, qos=1.0), ValueError),
        (client.direct.publish("a/b", {"a": 1}, retain=True), TypeError),
        (client.direct.publish("a/b", True, retain=True), TypeError),
        (client.direct.publish("a/b", "a\udcff", retain=True), TypeError),
        *(
            (client.direct.publish(topic, "x", retain=True), ValueError)
            for topic in [
                "a\0b",
                "a/\x1fb",
                "a\x7fb",
                "a/\x85",
                "a/\ufffe",
                "a\ud800",
                "a" * 65536,
                "/".join(["a"] * 202),
            ]
        ),
        (client.direct.subscribe("a/b", received.append, qos=3), ValueError),
        (client.direct.subscribe("a/b", received.append, qos=1.0), ValueError),
        *(
            (client.direct.subscribe(topic_filter, received.append), ValueError)
            for topic_filter in [
                "zigbee2mqtt/#/devices",
                "",
                "a/\0",
                "a/\x1f",
                "a/\x7f",
                "/".join(["+"] * 202),
                "a/b#",
                "a/+b",
                "+a",
                "#/a",
                "$share/group/a/#",
            ]
        ),
    ]

    for call, error in refused:
        with pytest.raises(error):
            await call

    assert internals(client) == before
    await asyncio.to_thread(broker.start if offline else broker.restart)
    await wait_until(lambda: client.is_connected, timeout=10)
    await wait_retained(broker.port, "/devices/dev/#", OWN_DEVICE)
    await client.direct.subscribe("zigbee2mqtt/#", received.append)
    await client.direct.publish("zigbee2mqtt/bridge/state", "online")
    await wait_until(lambda: len(received) == 1)


async def test_state_gone_with_a_restart_is_not_handed_over_after_reconnect(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    After reconnect a subscription knows only what the broker has now.

    A topic that left the broker with its restart does not reach a read of the subscribed filter.
    """
    await driver.direct.publish("/state/x", "1", retain=True)
    await driver.close()
    seen: list[Message] = []
    await client.direct.subscribe("/state/#", seen.append)
    await wait_until(lambda: len(seen) == 1)

    await restart_broker(broker, client)

    assert await wait_or_dump(client.direct.read("/state/#"), timeout=5) == {}


async def test_retained_command_left_while_offline_is_erased_after_reconnect(
    broker: MosquittoBroker, connect: ConnectClient, dead_link: tuple[DeadLinkProxy, Client]
) -> None:
    """
    A command left retained in /on while the link was down comes with the retained messages after
    reconnect. It is erased and not executed, a live command after it works.
    """
    proxy, client = dead_link
    relay = await (await client.create_device("dev")).create_control("relay", ControlType.SWITCH, value=False)
    commands: list[bool] = []
    relay.on_command(lambda control, value: commands.append(value))
    other = await connect("other")

    await proxy.drop(client)
    await other.direct.publish("/devices/dev/controls/relay/on", "1", retain=True)
    await proxy.restore(client)

    await wait_retained(broker.port, "/devices/dev/controls/relay/on", {})
    assert not commands
    await other.direct.publish("/devices/dev/controls/relay/on", "1")
    await wait_until(lambda: commands == [True])


async def test_direct_subscribe_without_a_link_returns_and_gets_retained_after_reconnect(
    driver: Client, dead_link: tuple[DeadLinkProxy, Client]
) -> None:
    """
    direct.subscribe without a link returns at once and keeps the subscription. What the broker keeps under
    the filter comes as retained after reconnect.
    """
    proxy, client = dead_link
    await driver.direct.publish("/state/x", "1", retain=True)
    received: list[Message] = []

    await proxy.drop(client)
    await wait_or_dump(client.direct.subscribe("/state/#", received.append), timeout=1)
    await proxy.restore(client)

    await wait_until(lambda: len(received) == 1)
    assert (received[0].topic, received[0].payload, received[0].retain) == ("/state/x", b"1", True)


async def test_direct_subscription_and_retained_publication_before_connect_take_effect_once_connected(
    broker: MosquittoBroker, driver: Client
) -> None:
    """
    Before connect() direct.subscribe and a retained direct.publish return at once, an event and a read
    of the broker raise BrokerDisconnectedError. Once the client connects, the handler gets what the broker
    keeps under its filter and the broker gets the retained message.
    """
    await driver.direct.publish("/state/x", "1", retain=True)
    client = Client("test-service", broker.url())
    received: list[Message] = []

    await wait_or_dump(client.direct.subscribe("/state/#", received.append), timeout=1)
    await wait_or_dump(client.direct.publish("/kept/x", "2", retain=True), timeout=1)
    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(client.direct.publish("/events/x", "1"), timeout=1)
    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(client.create_device("dev"), timeout=1)
    await wait_or_dump(client.connect(), timeout=5)

    await wait_until(lambda: len(received) == 1)
    assert (received[0].topic, received[0].payload, received[0].retain) == ("/state/x", b"1", True)
    await wait_retained(broker.port, "/kept/#", {"/kept/x": b"2"})
    await wait_or_dump(client.close(), timeout=5)


async def test_connection_change_handlers_see_every_change_after_registration(
    broker: MosquittoBroker,
) -> None:
    """
    A handler registered before connect() gets the first connection, then the loss and the return
    of the link around a broker restart. A run of failed attempts gives one False. A handler registered
    after connect() gets only the changes after it, and close() gives none.
    """
    client = Client("test-service", broker.url())
    early: list[bool] = []
    late: list[bool] = []
    client.on_connection_change(early.append)
    await wait_or_dump(client.connect(), timeout=5)
    client.on_connection_change(late.append)
    await wait_until(lambda: early == [True])

    await asyncio.to_thread(broker.stop)
    await wait_until(lambda: early == [True, False])
    # Several attempts fail while the broker is down
    await asyncio.sleep(2)
    await asyncio.to_thread(broker.start)
    await wait_until(lambda: early == [True, False, True], timeout=10)
    await wait_or_dump(client.close(), timeout=5)

    await asyncio.sleep(0.2)
    assert early == [True, False, True] and late == [False, True]


def test_connection_change_handler_can_be_registered_before_the_event_loop_runs(
    broker: MosquittoBroker,
) -> None:
    """
    A service may create the client and register the handler in synchronous code, before asyncio starts.
    """
    client = Client("test-service", broker.url())
    seen: list[bool] = []
    client.on_connection_change(seen.append)

    async def run() -> None:
        await wait_or_dump(client.connect(), timeout=5)
        await wait_until(lambda: seen == [True])
        await wait_or_dump(client.close(), timeout=5)

    asyncio.run(run())


async def test_replay_goes_on_after_the_broker_did_not_confirm_a_publication(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    After reconnect the client publishes its device again. The first publication gets no PUBACK in time
    while the link lives. It goes again, and the publications after it go out too.
    """
    device = await client.create_device("dev")
    for name in ("a", "b", "c"):
        await device.create_control(name, ControlType.VALUE, value=1)
    expected = await read_retained(broker.port, "/devices/dev/#")
    lost: list[str] = []

    async def lose_the_first_device_meta(send: Send, topic: str, _payload: Any) -> None:
        if topic == "/devices/dev/meta" and not lost:
            lost.append(topic)
            raise ack_timeout()
        await send()

    intercept_publish(monkeypatch, lose_the_first_device_meta)
    await restart_broker(broker, client)

    await wait_retained(broker.port, "/devices/dev/#", expected)
    assert lost == ["/devices/dev/meta"]


async def test_value_set_into_a_dead_link_goes_out_after_reconnect(
    broker: MosquittoBroker, dead_link: tuple[DeadLinkProxy, Client]
) -> None:
    """
    The link is dead and the client does not know it yet. set_value returns at once, the value goes into
    the dead link and then out with the next session.
    """
    proxy, client = dead_link
    temp = await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)

    proxy.dead = True
    await wait_or_dump(temp.set_value(2), timeout=1)
    # The packet reaches the dead link on the next turns of the loop
    await asyncio.sleep(0.2)
    proxy.dead = False
    proxy.cut()

    await wait_retained(broker.port, "/devices/dev/controls/temp", {"/devices/dev/controls/temp": b"2"}, 20)


@pytest.mark.parametrize("offline", [False, True], ids=["online", "offline"])
async def test_buffer_changed_after_publish_does_not_change_the_replay(
    broker: MosquittoBroker, dead_link: tuple[DeadLinkProxy, Client], offline: bool
) -> None:
    """
    A retained direct message published from a bytearray goes out again after reconnect as it was published,
    also when the service changes the buffer later.
    """
    proxy, client = dead_link
    if offline:
        await proxy.drop(client)
    buffer = bytearray(b"first")

    await client.direct.publish("/state/x", buffer, retain=True)
    buffer[:] = b"other"

    if offline:
        await proxy.restore(client)
    else:
        await restart_broker(broker, client)
    await wait_retained(broker.port, "/state/x", {"/state/x": b"first"})


@pytest.mark.parametrize("change", RETAINED_CHANGES)
async def test_retained_change_without_puback_in_time_goes_again_while_the_link_lives(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """
    The broker does not confirm a value, a meta or a direct retained message in time while the link lives.
    The call returns, and the change goes again over the same connection, without waiting for a reconnect.
    """
    call, changed, payload = RETAINED_CHANGES[change]
    temp = await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    changes: list[bool] = []
    client.on_connection_change(changes.append)
    lost: list[str] = []

    async def lose_the_first_publication(send: Send, topic: str, _payload: Any) -> None:
        if topic == changed and not lost:
            lost.append(topic)
            raise ack_timeout()
        await send()

    intercept_publish(monkeypatch, lose_the_first_publication)
    await wait_or_dump(call(client, temp), timeout=5)

    await wait_retained(broker.port, changed, {changed: payload})
    assert lost and not changes


async def test_retained_change_without_puback_in_time_goes_again_before_a_later_erasure_stage(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The broker does not confirm a meta change in time, and meanwhile remove() of another device queued its
    erasure stages. The meta goes again at once, it does not wait behind a stage the broker has not
    confirmed yet.
    """
    temp = await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    other = await client.create_device("other")
    await other.create_control("c", ControlType.VALUE, value=1)
    meta_lost = asyncio.Event()
    erasure_queued = asyncio.Event()
    release = asyncio.Event()

    async def lose_the_meta_and_hold_the_erasure(send: Send, topic: str, payload: Any) -> None:
        if topic == "/devices/dev/controls/temp/meta" and not meta_lost.is_set():
            meta_lost.set()
            await erasure_queued.wait()
            raise ack_timeout()
        await send()
        if topic.startswith("/devices/other/") and payload is None:
            await release.wait()

    intercept_publish(monkeypatch, lose_the_meta_and_hold_the_erasure)
    change = asyncio.ensure_future(temp.update_meta(units="V"))
    await wait_or_dump(meta_lost.wait(), timeout=2)
    removal = asyncio.ensure_future(other.remove())
    await asyncio.sleep(0.1)
    erasure_queued.set()

    meta_topic, meta = RETAINED_CHANGES["update_meta"][1:]
    await wait_retained(broker.port, meta_topic, {meta_topic: meta}, timeout=3)
    assert not removal.done()
    release.set()
    await wait_or_dump(asyncio.gather(change, removal), timeout=5)


async def test_value_overtaken_while_its_puback_is_late_does_not_go_again(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The service sets a value and a newer one before the broker confirmed the first, and the confirmation
    of the first does not come in time. The first value does not go again after the newer one, the broker
    keeps the newer.
    """
    temp = await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    sent: list[Any] = []
    newer_sent = asyncio.Event()

    async def time_out_the_older_value(send: Send, topic: str, payload: Any) -> None:
        if topic == "/devices/dev/controls/temp":
            sent.append(payload)
            if payload == "3":
                newer_sent.set()
        await send()
        if topic == "/devices/dev/controls/temp" and payload == "2" and sent.count("2") == 1:
            await newer_sent.wait()
            raise ack_timeout()

    intercept_publish(monkeypatch, time_out_the_older_value)
    await wait_or_dump(temp.set_value(2), timeout=1)
    await wait_or_dump(temp.set_value(3), timeout=1)

    await wait_retained(broker.port, "/devices/dev/controls/temp", {"/devices/dev/controls/temp": b"3"})
    await asyncio.sleep(0.2)
    assert sent == ["2", "3"]


async def test_queued_event_without_puback_in_time_fails_and_does_not_go_again(
    driver: Client, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Own values fill the window, a command to a foreign control waits in the queue behind them. The broker
    does not confirm the command in time while the link lives. The command ends with BrokerDisconnectedError
    and the client does not send it again.
    """
    relay = await (await driver.create_device("relays")).create_control("K1", ControlType.SWITCH, value=False)
    commands: list[bool] = []
    relay.on_command(lambda control, value: commands.append(value))
    remote = await client.get_control("relays/K1")
    controls = await window_of_controls(client)
    release = asyncio.Event()
    lost: list[str] = []

    async def hold_values_and_lose_the_command(send: Send, topic: str, _payload: Any) -> None:
        if topic == "/devices/relays/controls/K1/on" and not lost:
            lost.append(topic)
            raise ack_timeout()
        await send()
        if topic.startswith("/devices/dev/controls/"):
            await release.wait()

    intercept_publish(monkeypatch, hold_values_and_lose_the_command)
    for control in controls:
        await wait_or_dump(control.set_value(1), timeout=1)
    command = asyncio.ensure_future(remote.set_value(True))
    await asyncio.sleep(0.2)
    assert not command.done()
    release.set()

    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(command, timeout=5)
    await asyncio.sleep(0.5)
    assert lost and not commands


async def test_marker_confirmed_late_still_ends_a_read_of_the_broker(
    driver: Client, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The broker confirms the marker that ends retained delivery after every retained message before it,
    on a big delivery later than 5 s while the link lives. A read of the broker still returns
    what the broker keeps once the marker arrives.
    """
    await driver.direct.publish("/state/x", "1", retain=True)
    intercept_publish(monkeypatch, confirm_markers_late)

    assert await wait_or_dump(client.direct.read("/state/#"), timeout=5) == {"/state/x": b"1"}


async def test_marker_refused_by_paho_ends_a_read_with_broker_disconnected(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    paho refuses the marker that would end retained delivery, its link is broken. The read of the broker
    ends with BrokerDisconnectedError of the library, the error of the connection does not reach the service.
    """

    async def refuse_markers(send: Send, topic: str, _payload: Any) -> None:
        if topic.startswith("/wbretainhack/"):
            # MQTT_ERR_NO_CONN of paho
            raise aiomqtt.MqttCodeError(4, "Could not publish message")
        await send()

    intercept_publish(monkeypatch, refuse_markers)

    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(client.direct.read("/state/#"), timeout=5)


async def test_used_up_subscription_numbers_refuse_a_new_subscription_and_the_client_goes_on(
    dead_link: tuple[DeadLinkProxy, Client], monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The client has used up the subscription numbers of MQTT. A new subscription raises RuntimeError and does
    not stay in the client, also without a link, so the client still connects again after a lost link.
    """
    proxy, client = dead_link
    monkeypatch.setattr(
        # pylint: disable-next=protected-access
        client._subscriber,
        "_subscription_numbers",
        itertools.count(subscriber_module.MAX_SUBSCRIPTION_NUMBER + 1),
    )

    with pytest.raises(RuntimeError):
        await wait_or_dump(client.direct.subscribe("/a/#", print), timeout=5)
    await proxy.drop(client)
    with pytest.raises(RuntimeError):
        await wait_or_dump(client.direct.subscribe("/b/#", print), timeout=5)

    await proxy.restore(client)
    assert client.is_connected


async def test_session_that_keeps_failing_after_connack_keeps_the_pause_growing(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    After a broker restart every session fails right after CONNACK, before the client reports the link.
    The pause between the attempts keeps growing, as after a failed connect, and is not 0.5 s every time.
    """
    restore = subscriber_module.Subscriber.connection_restored
    attempts: list[float] = []
    healed = asyncio.Event()

    async def fail_after_connack(
        self: subscriber_module.Subscriber, connection: broker_connection.Connection
    ) -> None:
        if not healed.is_set():
            attempts.append(time.monotonic())
            raise broker_connection.LinkLost("the session breaks right after CONNACK")
        await restore(self, connection)

    monkeypatch.setattr(subscriber_module.Subscriber, "connection_restored", fail_after_connack)
    await asyncio.to_thread(broker.restart)
    await wait_until(lambda: bool(attempts), timeout=10)
    await asyncio.sleep(3.2)

    # The pause doubles from 1 s, so the fourth attempt comes 7 s after the first
    assert len(attempts) <= 3
    healed.set()
    await wait_until(lambda: client.is_connected, timeout=15)


async def test_acks_late_after_reconnect_do_not_drop_the_session(
    broker: MosquittoBroker, driver: Client, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    After reconnect the SUBACK of a subscription and the PUBACK of the marker come after the retained
    messages before them, on a big delivery later than 5 s. The session stays, the link is reported up,
    and the handler gets what the broker keeps again.
    """
    await driver.direct.publish("/state/x", "1", retain=True)
    received: list[Message] = []
    await client.direct.subscribe("/state/#", received.append)
    await wait_until(lambda: len(received) == 1)
    changes: list[bool] = []
    client.on_connection_change(changes.append)
    subscribe = aiomqtt.Client.subscribe

    async def confirm_the_subscription_late(
        self: aiomqtt.Client, topic_filter: str, *args: Any, **kwargs: Any
    ) -> None:
        await subscribe(self, topic_filter, *args, **kwargs)
        if topic_filter == "/state/#":
            raise ack_timeout()

    monkeypatch.setattr(aiomqtt.Client, "subscribe", confirm_the_subscription_late)
    intercept_publish(monkeypatch, confirm_markers_late)
    await asyncio.to_thread(broker.restart)

    await wait_until(lambda: changes == [False, True], timeout=15)
    await wait_until(lambda: len(received) == 2, timeout=10)
    assert received[1].topic == "/state/x" and client.is_connected


async def test_more_publications_than_paho_takes_at_once_go_out_whole_and_in_order(
    broker: MosquittoBroker, client: Client, publish_log: PublishLog
) -> None:
    """
    A burst of 30 values and the replay of a device with 30 controls wait for PUBACK more than 20 at once,
    while paho takes 20. Every publication reaches the broker, in the order of the calls.
    """
    device = await client.create_device("dev")
    controls = [await device.create_control(f"c{index}", ControlType.VALUE, value=0) for index in range(30)]
    publish_log.sent.clear()

    await wait_or_dump(asyncio.gather(*(control.set_value(1) for control in controls)), timeout=10)

    assert publish_log.sent == [(f"/devices/dev/controls/c{index}", "1") for index in range(30)]
    expected = await read_retained(broker.port, "/devices/dev/#")
    publish_log.slow = True
    await restart_broker(broker, client)
    await wait_retained(broker.port, "/devices/dev/#", expected, timeout=20)


async def test_device_removed_during_the_replay_is_not_sent_again_and_erased_in_stages(
    broker: MosquittoBroker, client: Client, publish_log: PublishLog
) -> None:
    """
    After reconnect the replay waits in the queue behind a full window, and the service removes a device
    whose replay has not gone out yet. Its old state is not sent again, the erasure goes in its stages
    with the device meta last.
    """
    first = await client.create_device("first")
    for index in range(15):
        await first.create_control(f"c{index}", ControlType.VALUE, value=1)
    second = await client.create_device("second")
    await second.create_control("c", ControlType.VALUE, value=1)
    publish_log.sent.clear()
    publish_log.slow = True
    await restart_broker(broker, client)

    await wait_or_dump(second.remove(), timeout=10)

    await wait_retained(broker.port, "/devices/second/#", {})
    assert [item for item in publish_log.sent if item[0].startswith("/devices/second/")] == [
        ("/devices/second/controls/c", None),
        ("/devices/second/controls/c/meta/error", None),
        ("/devices/second/controls/c/meta", None),
        ("/devices/second/meta", None),
    ]


async def test_values_set_during_the_replay_go_out_in_the_order_of_the_calls(
    broker: MosquittoBroker, client: Client, publish_log: PublishLog
) -> None:
    """
    After reconnect the replay waits in the queue behind a full window, and the service sets a value twice
    before the replay of its control goes out. The broker gets the two values in order, the replay does not
    send the newer one ahead of them.
    """
    device = await client.create_device("dev")
    for index in range(15):
        await device.create_control(f"c{index}", ControlType.VALUE, value=1)
    level = await device.create_control("level", ControlType.VALUE, value=1)
    publish_log.sent.clear()
    publish_log.slow = True
    await restart_broker(broker, client)

    await wait_or_dump(asyncio.gather(level.set_value(2), level.set_value(3)), timeout=10)

    assert [payload for topic, payload in publish_log.sent if topic == "/devices/dev/controls/level"] == [
        "2",
        "3",
    ]


@pytest.mark.parametrize("behind_an_erasure", [True, False], ids=["queued behind an erasure", "sent at once"])
async def test_event_when_the_link_drops_fails_at_once_and_does_not_go_later(
    connect: ConnectClient, dead_link: tuple[DeadLinkProxy, Client], behind_an_erasure: bool
) -> None:
    """
    An event waits in the queue behind a stage of an erasure the broker has not confirmed, or went
    to the connection with nothing before it, and the link drops before PUBACK. The event ends with
    BrokerDisconnectedError at once, not by the 5 s PUBACK timeout of aiomqtt, and does not go out
    after reconnect.
    """
    proxy, client = dead_link
    device = await client.create_device("dev")
    await device.create_control("temp", ControlType.VALUE, value=1)
    watcher = await connect("watcher")
    events: list[Message] = []
    await watcher.direct.subscribe("/events/#", events.append)

    proxy.dead = True
    removal = asyncio.ensure_future(device.remove()) if behind_an_erasure else None
    event = asyncio.ensure_future(client.direct.publish("/events/x", "1"))
    await asyncio.sleep(0.2)
    assert not event.done()
    proxy.cut()

    with pytest.raises(BrokerDisconnectedError):
        await wait_or_dump(event, timeout=1)
    await proxy.restore(client)
    if removal is not None:
        await wait_or_dump(removal, timeout=10)
    await asyncio.sleep(0.5)
    assert not events


async def test_own_publication_that_fails_unexpectedly_leaves_the_mirror_and_the_rest_goes_on(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A bug breaks one publication of the replay with an unexpected error. That message leaves the mirror,
    the rest of the own state still reaches the broker.
    """
    device = await client.create_device("dev")
    for name in ("a", "b", "c"):
        await device.create_control(name, ControlType.VALUE, value=1)
    expected = await read_retained(broker.port, "/devices/dev/#")
    mirrored = internals(client)["replay mirror"]
    broken: list[str] = []

    async def break_one_publication(send: Send, topic: str, _payload: Any) -> None:
        if topic == "/devices/dev/controls/b" and not broken:
            broken.append(topic)
            raise RuntimeError("a bug")
        await send()

    intercept_publish(monkeypatch, break_one_publication)
    await restart_broker(broker, client)

    del expected["/devices/dev/controls/b"]
    await wait_retained(broker.port, "/devices/dev/#", expected)
    assert broken and internals(client)["replay mirror"] == mirrored - 1


async def test_session_that_fails_unexpectedly_is_started_again(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A bug breaks the start of a session after a broker restart. The client connects again by itself,
    its device goes back to the broker.
    """
    await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=21)
    restore = publisher_module.Publisher.connection_restored
    broken: list[broker_connection.Connection] = []

    def break_the_first_restore(
        self: publisher_module.Publisher, connection: broker_connection.Connection
    ) -> None:
        if not broken:
            broken.append(connection)
            raise RuntimeError("a bug")
        restore(self, connection)

    monkeypatch.setattr(publisher_module.Publisher, "connection_restored", break_the_first_restore)
    await asyncio.to_thread(broker.restart)

    await wait_retained(broker.port, "/devices/dev/#", OWN_DEVICE, timeout=20)
    assert broken and client.is_connected
