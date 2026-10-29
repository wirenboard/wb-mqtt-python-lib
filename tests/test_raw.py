"""
client.raw: topics outside the convention.
"""

import asyncio
from typing import Any

import pytest

from tests.broker import (
    DeadLinkProxy,
    MosquittoBroker,
    Send,
    intercept_publish,
    internals,
    lose_end_markers,
    wait_or_dump,
    wait_until,
)
from wb_mqtt_lib import Client, ControlType, Message
from wb_mqtt_lib._mqtt import subscriber as subscriber_module


async def test_subscribe_gets_retained_at_once_then_live(client: Client) -> None:
    """
    A raw subscription gets what lies in the broker, then live messages, each with the retain flag it was
    published with.
    """
    await client.raw.publish("zigbee2mqtt/bridge/state", '{"state": "online"}', retain=True)
    received: list[Message] = []

    await client.raw.subscribe("zigbee2mqtt/#", received.append)
    await client.raw.publish("zigbee2mqtt/bridge/request/permit_join", '{"time": 254}')
    await client.raw.publish("zigbee2mqtt/bridge/state", '{"state": "offline"}', retain=True)

    await wait_until(lambda: len(received) == 3)
    assert [(m.topic, m.retain) for m in received] == [
        ("zigbee2mqtt/bridge/state", True),
        ("zigbee2mqtt/bridge/request/permit_join", False),
        ("zigbee2mqtt/bridge/state", True),
    ]


async def test_read_and_erase_recipe(broker: MosquittoBroker, client: Client) -> None:
    """
    The recipe from the README erases topics by a mask.

    Erasures do not stay in the replay mirror, a one-off read leaves no subscription in the broker.
    """
    for name in ("a", "b"):
        await client.raw.publish(f"/devices/old/controls/{name}", "1", retain=True)

    for topic in await client.raw.read("/devices/old/#"):
        await client.raw.publish(topic, "", retain=True)

    assert await client.raw.read("/devices/old/#") == {}
    assert internals(client)["replay mirror"] == 0
    assert broker.unsubscriptions() == ["/devices/old/#"] * 2


async def test_one_handler_on_several_filters_until_removed(broker: MosquittoBroker, client: Client) -> None:
    """
    One handler listens to several filters with one queue, overlapping filters call it once.

    raw.unsubscribe takes it off every filter and the broker subscriptions go with it.
    """
    received: list[str] = []

    def handler(message: Message) -> None:
        received.append(message.topic)

    for topic_filter in ("zigbee2mqtt/bridge/devices", "zigbee2mqtt/bridge/#", "zigbee2mqtt/bridge/groups"):
        assert await client.raw.subscribe(topic_filter, handler) is handler
    await client.raw.subscribe("zigbee2mqtt/bridge/devices", handler)
    for name in ("devices", "groups", "state"):
        await client.raw.publish(f"zigbee2mqtt/bridge/{name}", "[]")
    await wait_until(lambda: len(received) == 3)

    await client.raw.unsubscribe(handler)
    after: list[str] = []
    await client.raw.subscribe("zigbee2mqtt/#", lambda message: after.append(message.topic))
    await client.raw.publish("zigbee2mqtt/bridge/devices", "[]")
    await wait_until(lambda: len(after) == 1)

    assert received == ["zigbee2mqtt/bridge/devices", "zigbee2mqtt/bridge/groups", "zigbee2mqtt/bridge/state"]
    assert internals(client)["subscriptions"] == 1
    assert sorted(broker.unsubscriptions()) == [
        "zigbee2mqtt/bridge/#",
        "zigbee2mqtt/bridge/devices",
        "zigbee2mqtt/bridge/groups",
    ]


async def test_unsubscribe_cancelled_midway_still_takes_the_handler_off_every_filter(client: Client) -> None:
    """
    The task that unsubscribes is cancelled while the broker answers the first filter.

    The handler still goes off the other filters and gets nothing more.
    """
    received: list[str] = []
    baseline = internals(client)

    def handler(message: Message) -> None:
        received.append(message.topic)

    for topic_filter in ("a/#", "b/#", "c/#"):
        await client.raw.subscribe(topic_filter, handler)
    call = asyncio.ensure_future(client.raw.unsubscribe(handler))
    await asyncio.sleep(0)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call

    await wait_until(lambda: internals(client)["subscriptions"] == baseline["subscriptions"])
    await client.raw.publish("c/x", "1")
    await asyncio.sleep(0.2)
    assert not received and internals(client) == baseline


@pytest.mark.parametrize("filter_held", [False, True], ids=["new filters", "filter held by another handler"])
async def test_one_handler_on_overlapping_filters_gets_a_retained_message_once(
    client: Client, filter_held: bool
) -> None:
    """
    One handler subscribes to two overlapping filters, a retained message under both reaches it once.

    The second filter may be new or held by another handler already, then the handler joins its subscription.
    """
    await client.raw.publish("zigbee2mqtt/bridge/devices", "[]", retain=True)
    if filter_held:
        await client.raw.subscribe("zigbee2mqtt/bridge/#", lambda message: None)
    received: list[Message] = []

    await client.raw.subscribe("zigbee2mqtt/bridge/devices", received.append)
    await client.raw.subscribe("zigbee2mqtt/bridge/#", received.append)

    await asyncio.sleep(0.2)
    assert [(m.topic, m.retain) for m in received] == [("zigbee2mqtt/bridge/devices", True)]


async def test_one_handler_on_overlapping_filters_gets_a_retained_message_once_after_reconnect(
    driver: Client, dead_link: tuple[DeadLinkProxy, Client]
) -> None:
    """
    After reconnect both overlapping filters of one handler are subscribed again at once. A retained message
    under both comes to the handler once more, as retained and once.
    """
    proxy, client = dead_link
    await driver.raw.publish("zigbee2mqtt/bridge/devices", "[]", retain=True)
    received: list[Message] = []
    await client.raw.subscribe("zigbee2mqtt/bridge/devices", received.append)
    await client.raw.subscribe("zigbee2mqtt/bridge/#", received.append)
    await wait_until(lambda: len(received) == 1)

    await proxy.drop(client)
    await proxy.restore(client)

    await asyncio.sleep(0.5)
    assert [(m.topic, m.retain) for m in received] == [("zigbee2mqtt/bridge/devices", True)] * 2


async def test_handler_joining_a_held_filter_while_its_other_filter_waits_for_the_end_gets_a_message_once(
    driver: Client, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A handler subscribes to a wide filter, the retained message under it comes, and the end of the delivery
    does not. Meanwhile the handler joins a narrow filter that another handler holds already. The message
    under both reaches it once.
    """
    await driver.raw.publish("zigbee2mqtt/bridge/devices", "[]", retain=True)
    await client.raw.subscribe("zigbee2mqtt/bridge/#", lambda message: None)
    lose_end_markers(client, monkeypatch)
    monkeypatch.setattr(subscriber_module, "RETAINED_END_TIMEOUT_S", 0.5)
    received: list[str] = []

    def handler(message: Message) -> None:
        received.append(message.topic)

    wide = asyncio.ensure_future(client.raw.subscribe("zigbee2mqtt/#", handler))
    await wait_until(lambda: received == ["zigbee2mqtt/bridge/devices"])
    await wait_or_dump(client.raw.subscribe("zigbee2mqtt/bridge/#", handler), timeout=5)
    await wait_or_dump(wide, timeout=5)
    # One queue per handler, a message handed over twice would come before this one
    await driver.raw.publish("zigbee2mqtt/bridge/state", "online")

    await wait_until(lambda: received[-1] == "zigbee2mqtt/bridge/state")
    assert received == ["zigbee2mqtt/bridge/devices", "zigbee2mqtt/bridge/state"]


@pytest.mark.parametrize("together", [False, True], ids=["one after another", "at once"])
async def test_handlers_on_one_filter_get_every_retained_message_once(client: Client, together: bool) -> None:
    """
    Three handlers subscribe to one filter one after another or at once,
    each gets every retained message once.

    The first one takes them from the broker, the others from the subscription, also while the broker
    is still sending them to the first one.
    """
    topics = [f"/state/t{index}" for index in range(200)]
    for topic in topics:
        await client.raw.publish(topic, "1", retain=True)
    received: list[list[Message]] = [[], [], []]

    subscriptions = [client.raw.subscribe("/state/#", messages.append) for messages in received]
    if together:
        await asyncio.gather(*subscriptions)
    else:
        for subscription in subscriptions:
            await subscription

    await wait_until(lambda: all(len(messages) >= len(topics) for messages in received))
    await asyncio.sleep(0.2)
    for messages in received:
        assert sorted(m.topic for m in messages) == sorted(topics) and all(m.retain for m in messages)


async def test_second_handler_and_a_read_of_a_subscribed_filter_need_no_broker(
    driver: Client, dead_link: tuple[DeadLinkProxy, Client]
) -> None:
    """
    A second handler on a subscribed filter and a read of that filter get the state from the subscription.

    The link loses everything meanwhile, and both return at once. The subscription follows live retained
    messages, so a topic published after it is there too, a kept topic has its newer value, and a topic
    erased after it is gone.
    """
    await driver.raw.publish("/state/x", "1", retain=True)
    await driver.raw.publish("/state/z", "1", retain=True)
    proxy, client = dead_link
    first: list[Message] = []
    await client.raw.subscribe("/state/#", first.append)
    await driver.raw.publish("/state/x", "3", retain=True)
    await driver.raw.publish("/state/y", "2", retain=True)
    await driver.raw.publish("/state/z", "", retain=True)
    await wait_until(lambda: len(first) == 5)
    proxy.dead = True
    second: list[Message] = []

    await wait_or_dump(client.raw.subscribe("/state/#", second.append), timeout=2)
    read = await wait_or_dump(client.raw.read("/state/#"), timeout=2)

    proxy.dead = False
    assert read == {"/state/x": b"3", "/state/y": b"2"}
    await wait_until(lambda: len(second) == 2)
    assert sorted((m.topic, m.payload, m.retain) for m in second) == [
        ("/state/x", b"3", True),
        ("/state/y", b"2", True),
    ]


async def test_raw_handler_over_a_read_device_gets_no_repeats_and_the_device_no_double_events(
    driver: Client, client: Client
) -> None:
    """
    A raw handler on /devices/# overlaps the subscription of a device the same client reads.

    The broker sends a copy of a message to each subscription. Reading the device brings the raw handler
    no retained messages again, a live message reaches it once, and a press reaches the device once.
    """
    temp = await (await driver.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    await driver.raw.publish("/devices/dev/controls/button/meta", '{"type": "pushbutton"}', retain=True)
    seen: list[Message] = []
    await client.raw.subscribe("/devices/#", seen.append)
    await wait_until(lambda: len(seen) == 4)
    seen.clear()
    button = await client.get_control("dev/button")
    remote = await client.get_control("dev/temp")
    presses: list = []
    button.on_value(lambda control, value, previous: presses.append(value))

    await temp.set_value(2)
    await driver.raw.publish("/devices/dev/controls/button", "1")

    await wait_until(lambda: presses == [True] and remote.value == 2)
    await asyncio.sleep(0.2)
    assert presses == [True]
    assert [(m.topic, m.payload, m.retain) for m in seen] == [
        ("/devices/dev/controls/temp", b"2", True),
        ("/devices/dev/controls/button", b"1", False),
    ]


async def test_read_leaves_out_a_topic_erased_before_its_retained_messages_end(
    driver: Client, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A topic is erased after the broker delivered it to a one-off read but before the marker that ends
    the delivery. The read gives what the broker keeps by the end, without that topic.
    """
    await driver.raw.publish("/state/x", "1", retain=True)
    await driver.raw.publish("/state/y", "2", retain=True)
    erased: list[str] = []

    async def erase_before_the_marker(send: Send, topic: str, _payload: Any) -> None:
        if topic.startswith("/wbretainhack/") and not erased:
            erased.append(topic)
            await driver.raw.publish("/state/x", "", retain=True)
        await send()

    intercept_publish(monkeypatch, erase_before_the_marker)

    assert await wait_or_dump(client.raw.read("/state/#"), timeout=5) == {"/state/y": b"2"}
    assert erased


async def test_failed_subscribe_of_a_listening_handler_keeps_its_filters(client: Client) -> None:
    """
    A raw subscribe of a handler that listens already fails, on its own filter with a QoS the broker
    refuses or on a filter the broker refuses. The handler keeps getting the messages of its filter.
    """
    received: list[str] = []

    def handler(message: Message) -> None:
        received.append(message.topic)

    await client.raw.subscribe("/a/#", handler)
    with pytest.raises(ValueError):
        await client.raw.subscribe("/a/#", handler, qos=3)
    with pytest.raises(ValueError):
        await client.raw.subscribe("/b/x#", handler)
    await client.raw.publish("/a/x", "1")

    await wait_until(lambda: received == ["/a/x"])


@pytest.mark.parametrize("added_again", [False, True], ids=["taken off", "taken off and added again"])
async def test_subscribe_cancelled_after_unsubscribe_of_its_handler_changes_nothing(
    client: Client, monkeypatch: pytest.MonkeyPatch, added_again: bool
) -> None:
    """
    unsubscribe takes a handler off while its raw subscribe still waits for the broker, and another
    subscribe may add the handler again meanwhile. Then the first subscribe is cancelled. It ends with
    CancelledError and changes nothing, the handler gets the messages of the filter it has now and of no
    other.
    """
    received: list[str] = []
    baseline = internals(client)

    def handler(message: Message) -> None:
        received.append(message.topic)

    lose_end_markers(client, monkeypatch)
    first = asyncio.ensure_future(client.raw.subscribe("/a/#", handler))
    await wait_until(lambda: internals(client)["subscriptions"] == baseline["subscriptions"] + 1)
    await client.raw.unsubscribe(handler)
    second = None
    if added_again:
        second = asyncio.ensure_future(client.raw.subscribe("/b/#", handler))
        await wait_until(lambda: internals(client)["subscriptions"] == baseline["subscriptions"] + 1)

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    monkeypatch.undo()
    if second is not None:
        # The marker of a read ends the wait of the second subscribe too
        await client.raw.read("/c/#")
        await wait_or_dump(second, timeout=5)
    await client.raw.publish("/a/x", "1")
    await client.raw.publish("/b/x", "1")

    await asyncio.sleep(0.2)
    held = 1 if added_again else 0
    assert received == (["/b/x"] if added_again else [])
    assert internals(client)["subscriptions"] == baseline["subscriptions"] + held
    assert internals(client)["listeners"] == baseline["listeners"] + held


async def test_handler_subscribed_again_while_its_first_subscription_is_on_the_way_waits_for_it(
    driver: Client, client: Client
) -> None:
    """
    The same handler on the same filter again, while the first SUBSCRIBE is still on its way. The second
    call returns only once the retained messages are queued for the handler, each message comes once.
    """
    topics = [f"/state/t{index}" for index in range(50)]
    for topic in topics:
        await driver.raw.publish(topic, "1", retain=True)
    received: list[str] = []

    def handler(message: Message) -> None:
        received.append(message.topic)

    first = asyncio.ensure_future(client.raw.subscribe("/state/#", handler))
    await asyncio.sleep(0)
    await wait_or_dump(client.raw.subscribe("/state/#", handler), timeout=5)
    for _ in range(3):
        await asyncio.sleep(0)

    assert sorted(received) == sorted(topics)
    await wait_or_dump(first, timeout=5)
    await asyncio.sleep(0.2)
    assert len(received) == len(topics)
