"""
Objects by name over the device manager of the client and who a device name belongs to.
"""

import asyncio
import json
from typing import Any, Optional

import pytest

from tests.broker import (
    FIRST_VALUE_META,
    DeadLinkProxy,
    MosquittoBroker,
    Send,
    cancel_and_wait,
    device_object,
    intercept_publish,
    internals,
    publish_retained,
    read_retained,
    restart_broker,
    wait_or_dump,
    wait_retained,
    wait_until,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import (
    Client,
    Control,
    ControlErrorFlags,
    ControlType,
    Device,
    DeviceTakenError,
)

# What another driver may keep under a name the service wants
NAME_HOLDERS = pytest.mark.parametrize(
    "topics",
    [
        {"/devices/X/meta": '{"driver": "other-driver"}', "/devices/X/controls/a/meta": '{"type": "value"}'},
        {"/devices/X/meta": '{"title": {"en": "No Driver"}}'},
        {"/devices/X/meta/name": "Legacy", "/devices/X/controls/state/meta": '{"type": "text"}'},
        {
            "/devices/X/meta/driver": "other-driver",
            "/devices/X/meta/name": "Legacy",
            "/devices/X/controls/a/meta/type": "value",
            "/devices/X/controls/a": "1",
        },
        {
            "/devices/X/meta": '{"driver": "other-driver"}',
            "/devices/X/meta/driver": "test-service",
            "/devices/X/controls/a/meta": '{"type": "value"}',
        },
    ],
    ids=[
        "another driver",
        "meta without driver",
        "controls only",
        "legacy /meta/driver of another driver",
        "meta of another driver over a stale /meta/driver",
    ],
)


async def test_objects_of_one_control_are_equal_and_see_one_state(driver: Client, client: Client) -> None:
    """
    get_control, device.controls and the argument of a handler give equal objects, a listener works through
    any. Objects of another client under the same name are not equal. The service does not build
    the objects itself, the constructors raise.
    """
    temp = await (await driver.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    first = await client.get_control("dev/temp")
    second = (await client.get_device("dev")).controls["temp"]
    seen = []
    second.on_value(lambda control, value, previous: seen.append((control, value)))

    await temp.set_value(5)

    await wait_until(lambda: seen[-1:] == [(first, 5)])
    argument = seen[-1][0]
    assert first == second == argument and len({first, second, argument}) == 1
    assert first.value == second.value == argument.value == 5
    assert temp != first and temp.device != first.device
    with pytest.raises(TypeError):
        Device("dev")
    with pytest.raises(TypeError):
        Control(first.device, "temp")


@pytest.mark.parametrize("new_owner", ["driver", "service"])
async def test_objects_and_listeners_follow_a_name_that_comes_back(
    driver: Client, client: Client, new_owner: str
) -> None:
    """
    A foreign device was read, its driver erased it, and the name comes back. The driver publishes it again,
    as libwbmqtt1 drivers do on restart, or the service creates its own.

    The same objects and listeners see the removal and then the control that came back.
    """
    device = await driver.create_device("X")
    await device.create_control("temp", ControlType.VALUE, value=1)
    temp = await client.get_control("X/temp")
    events = []
    temp.on_added(lambda control: events.append("added"))
    temp.on_removed(lambda control: events.append("removed"))
    temp.on_value(lambda control, value, previous: events.append(f"value {value} after {previous}"))
    await wait_until(lambda: len(events) == 2)

    await device.remove()
    await wait_until(lambda: not temp.exists)
    owner = driver if new_owner == "driver" else client
    await (await owner.create_device("X")).create_control("temp", ControlType.VALUE, value=2)

    await wait_until(lambda: len(events) == 5)
    assert events == ["added", "value 1 after None", "removed", "added", "value 2 after None"]
    assert temp.is_own is (new_owner == "service")
    assert await client.get_control("X/temp") == temp


async def test_removed_own_name_can_go_to_another_driver(driver: Client, client: Client) -> None:
    """
    After remove() the name is free. The service sees the device of another driver as foreign.
    """
    own = await client.create_device("X")
    await own.create_control("temp", ControlType.VALUE, value=1)
    await own.remove()
    await own.remove()  # a second call does nothing

    await (await driver.create_device("X")).create_control("temp", ControlType.VALUE, value=7)
    temp = await client.get_control("X/temp", timeout=5)

    assert not temp.is_own and temp.value == 7 and temp.device.driver == "driver"


@NAME_HOLDERS
async def test_create_device_leaves_a_device_of_another_driver_alone(
    broker: MosquittoBroker, driver: Client, client: Client, topics: dict[str, str]
) -> None:
    """
    The name belongs to another device. Nothing is published and no subscription stays.
    """
    await publish_retained(driver, topics)
    before = await read_retained(broker.port, "/devices/X/#")

    with pytest.raises(DeviceTakenError):
        await client.create_device("X")

    assert await read_retained(broker.port, "/devices/X/#") == before
    assert internals(client)["subscriptions"] == 0


@NAME_HOLDERS
async def test_create_device_with_force_replaces_a_device_of_another_driver(
    broker: MosquittoBroker, driver: Client, client: Client, topics: dict[str, str]
) -> None:
    """
    force erases everything under the name, legacy subtopics too, and publishes the own device.
    """
    await publish_retained(driver, topics)

    device = await client.create_device("X", title="Mine", force=True)
    await device.create_control("temp", ControlType.VALUE, value=1)

    retained = await read_retained(broker.port, "/devices/X/#")
    assert sorted(retained) == [
        "/devices/X/controls/temp",
        "/devices/X/controls/temp/meta",
        "/devices/X/meta",
    ]
    assert json.loads(retained["/devices/X/meta"]) == {"driver": "test-service", "title": {"en": "Mine"}}
    assert device.is_own and list(device.controls) == ["temp"]


async def test_create_device_starts_clean_over_leftovers_of_its_own_driver(
    broker: MosquittoBroker, connect: ConnectClient
) -> None:
    """
    Leftovers of a previous run do not block the name and are erased first.

    A control the service does not create this time does not stay in the broker.
    """
    previous_run = await connect("service")
    old = await previous_run.create_device("X", title="Old")
    await old.create_control("old", ControlType.VALUE, value=1)
    await previous_run.close()

    client = await connect("service")
    device = await client.create_device("X", title="New")
    await device.create_control("new", ControlType.VALUE, value=2)

    assert list(device.controls) == ["new"]
    retained = await read_retained(broker.port, "/devices/X/#")
    assert json.loads(retained["/devices/X/meta"]) == {"driver": "service", "title": {"en": "New"}}
    assert not [topic for topic in retained if "/controls/old" in topic]


@pytest.mark.parametrize(
    "change",
    [
        {
            "/devices/dev/meta": "",
            "/devices/dev/controls/ch1/meta": "",
            "/devices/dev/controls/ch1/meta/error": "",
            "/devices/dev/controls/ch1": "",
        },
        {
            "/devices/dev/meta": '{"title": {"en": "Other"}}',
            "/devices/dev/controls/ch1/meta": '{"type": "text", "title": {"en": "X"}}',
        },
        {"/devices/dev/meta": '{"driver": "intruder"}'},
    ],
    ids=["erased like homeui does", "meta changed", "another driver"],
)
async def test_outside_change_of_own_device_lasts_until_reconnect(
    broker: MosquittoBroker, connect: ConnectClient, client: Client, change: dict[str, str]
) -> None:
    """
    The library does not follow own devices in the broker, like wb-rules and libwbmqtt1.

    An outside change stays and the objects keep the own state. After reconnect the service publishes
    its devices again and the change is gone.
    """
    device = await client.create_device("dev", title="Mine")
    channel = await device.create_control("ch1", ControlType.VALUE, title="Channel", value=12.5)
    await channel.set_error(ControlErrorFlags.READ)
    error_topic = "/devices/dev/controls/ch1/meta/error"
    # The error goes out last, once it is in the broker the rest is too
    await wait_retained(broker.port, error_topic, {error_topic: b"r"})
    before = await read_retained(broker.port, "/devices/dev/#")
    other = await connect("other")
    await publish_retained(other, change)
    # Its raw retained would come back after the broker restart
    await other.close()
    changed = {topic: payload for topic, payload in before.items() if topic not in change}
    changed.update({topic: payload.encode() for topic, payload in change.items() if payload})

    await wait_retained(broker.port, "/devices/dev/#", changed)
    await asyncio.sleep(1)
    assert await read_retained(broker.port, "/devices/dev/#") == changed
    assert device.is_own and device.driver == "test-service" and channel.exists and channel.value == 12.5

    await restart_broker(broker, client)
    await wait_retained(broker.port, "/devices/dev/#", before)


@pytest.mark.parametrize("value", [None, 6], ids=["without value", "with value"])
async def test_control_created_again_over_a_previous_run_starts_clean(
    broker: MosquittoBroker, connect: ConnectClient, value: Optional[int]
) -> None:
    """
    The old value and error of a previous run do not stay under a control created again.

    Without that set_error(NONE) would find nothing to clear and the old error would stay forever.
    """
    previous_run = await connect("service")
    old = await (await previous_run.create_device("X")).create_control("temp", ControlType.VALUE, value=5)
    await old.set_error(ControlErrorFlags.READ)
    await previous_run.close()
    client = await connect("service")

    temp = await (await client.create_device("X")).create_control("temp", ControlType.VALUE, value=value)

    expected = {"/devices/X/controls/temp": b"6"} if value else {}
    assert await read_retained(broker.port, "/devices/X/controls/temp/#") == {
        "/devices/X/controls/temp/meta": FIRST_VALUE_META,
        **expected,
    }
    assert temp.value == value and temp.error == ControlErrorFlags.NONE
    await temp.set_error(ControlErrorFlags.READ)
    await temp.set_error(ControlErrorFlags.NONE)
    await temp.set_value(7)
    await wait_retained(
        broker.port,
        "/devices/X/controls/temp/#",
        {
            "/devices/X/controls/temp/meta": FIRST_VALUE_META,
            "/devices/X/controls/temp": b"7",
        },
    )


async def test_refused_name_is_free_after_the_other_device_is_gone(driver: Client, client: Client) -> None:
    """
    create_device refused while another driver held the name works once that device is gone.
    """
    other_device = await driver.create_device("X")
    await other_device.create_control("temp", ControlType.VALUE, value=1)
    with pytest.raises(DeviceTakenError):
        await client.create_device("X")

    await other_device.remove()

    device = await wait_or_dump(client.create_device("X"), timeout=5)
    assert device.is_own and device.driver == "test-service"


async def test_objects_follow_a_watched_name_through_every_owner(driver: Client, client: Client) -> None:
    """
    A name the service reads goes from another driver to the service and back. The objects follow it
    without a new get_control, also after a refused create_device.
    """
    foreign = await driver.create_device("X")
    await foreign.create_control("temp", ControlType.VALUE, value=1)
    temp = await client.get_control("X/temp")

    with pytest.raises(DeviceTakenError):
        await client.create_device("X")
    assert temp.exists and temp.value == 1
    await foreign.remove()
    await wait_until(lambda: not temp.exists)
    own = await client.create_device("X")
    await own.create_control("temp", ControlType.VALUE, value=5)
    assert temp.is_own and temp.value == 5
    await own.remove()
    again = await driver.create_device("X")
    await again.create_control("temp", ControlType.VALUE, value=7)

    await wait_until(lambda: temp.exists and temp.value == 7)
    assert not temp.is_own and temp.device.driver == "driver"


async def test_create_device_cancelled_at_any_point_leaves_the_name_usable(
    broker: MosquittoBroker, client: Client
) -> None:
    """
    Cancelled before, during or after the broker check, create_device leaves the name either free with
    nothing in the broker, or own with the device published and its commands subscribed.
    """
    for delay in (0, 0.001, 0.003, 0.01, 0.03, 0.1):
        attempt = asyncio.ensure_future(client.create_device("X"))
        await asyncio.sleep(delay)
        await cancel_and_wait(attempt)
        cancelled = device_object(client, "X")
        if cancelled.is_own:
            await wait_retained(
                broker.port, "/devices/X/#", {"/devices/X/meta": b'{"driver": "test-service"}'}
            )
            await wait_until(lambda: internals(client)["subscriptions"] == 1)
            await cancelled.remove()
        else:
            assert await read_retained(broker.port, "/devices/X/#") == {}, delay
        await wait_until(lambda: internals(client)["subscriptions"] == 0)

        device = await wait_or_dump(client.create_device("X"), timeout=5)

        assert device.is_own, delay
        await device.remove()
    assert internals(client)["subscriptions"] == 0


async def test_create_device_cancelled_while_its_leftovers_are_erased_still_ends_own(
    broker: MosquittoBroker, connect: ConnectClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The caller of create_device is cancelled while the leftovers of a previous run are erased. The device
    still becomes own, published in their place, with its commands subscribed.
    """
    previous_run = await connect("service")
    await (await previous_run.create_device("X")).create_control("old", ControlType.VALUE, value=1)
    await previous_run.close()
    client = await connect("service")
    attempt = asyncio.ensure_future(client.create_device("X"))

    async def cancel_the_caller_at_an_erasure(send: Send, topic: str, payload: Any) -> None:
        if topic.startswith("/devices/X/") and payload is None:
            attempt.cancel()
        await send()

    intercept_publish(monkeypatch, cancel_the_caller_at_an_erasure)
    with pytest.raises(asyncio.CancelledError):
        await wait_or_dump(attempt, timeout=5)

    await wait_until(lambda: device_object(client, "X").is_own)
    await wait_retained(broker.port, "/devices/X/#", {"/devices/X/meta": b'{"driver": "service"}'})
    await wait_until(lambda: internals(client)["subscriptions"] == 1)


@pytest.mark.parametrize("cancelled", [False, True], ids=["awaited", "cancelled"])
async def test_create_device_ends_own_when_the_link_drops_while_its_leftovers_are_erased(
    broker: MosquittoBroker, connect: ConnectClient, monkeypatch: pytest.MonkeyPatch, cancelled: bool
) -> None:
    """
    The link drops while create_device erases the leftovers of a previous run. The name is checked already,
    so the device becomes own all the same, for a cancelled caller too. The erasure and the meta go out
    after reconnect.
    """
    previous_run = await connect("service")
    await (await previous_run.create_device("X")).create_control("old", ControlType.VALUE, value=1)
    await previous_run.close()
    proxy = DeadLinkProxy(broker.port)
    await proxy.start()
    client = Client("service", proxy.url())
    await wait_or_dump(client.connect(), timeout=5)
    try:
        attempt = asyncio.ensure_future(client.create_device("X"))
        # Once, the replay after reconnect sends the erasure again
        dropped: list[str] = []

        async def drop_the_link_at_an_erasure(send: Send, topic: str, payload: Any) -> None:
            if topic.startswith("/devices/X/") and payload is None and not dropped:
                dropped.append(topic)
                proxy.dead = True
                proxy.cut()
                if cancelled:
                    attempt.cancel()
            await send()

        intercept_publish(monkeypatch, drop_the_link_at_an_erasure)
        if cancelled:
            with pytest.raises(asyncio.CancelledError):
                await wait_or_dump(attempt, timeout=10)
        else:
            assert (await wait_or_dump(attempt, timeout=10)).is_own

        await wait_until(lambda: device_object(client, "X").is_own)
        await proxy.restore(client)
        await wait_retained(broker.port, "/devices/X/#", {"/devices/X/meta": b'{"driver": "service"}'})
    finally:
        await wait_or_dump(client.close(), timeout=10)
        await proxy.close()


async def test_two_creates_of_one_name_over_leftovers_make_one_device(
    broker: MosquittoBroker, connect: ConnectClient
) -> None:
    """
    Two create_device of one name at once over leftovers of a previous run. Both pass the check of the name,
    one takes it after the erasure, the other gets ValueError and does not publish over it.
    """
    previous_run = await connect("service")
    await (await previous_run.create_device("X")).create_control("old", ControlType.VALUE, value=1)
    await previous_run.close()
    client = await connect("service")

    results = await wait_or_dump(
        asyncio.gather(client.create_device("X"), client.create_device("X"), return_exceptions=True),
        timeout=5,
    )

    assert sorted(type(result).__name__ for result in results) == ["Device", "ValueError"]
    await wait_retained(broker.port, "/devices/X/#", {"/devices/X/meta": b'{"driver": "service"}'})
    assert internals(client)["subscriptions"] == 1


async def test_device_created_over_a_foreign_one_gone_unnoticed_starts_clean(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    The broker lost a foreign device while its driver was gone, and the client did not notice, as wb-rules
    would not. An own device created under the name keeps nothing of it, neither the error of the device
    nor the value of a control without meta.
    """
    await publish_retained(
        driver,
        {
            "/devices/dev/meta": '{"driver": "driver"}',
            "/devices/dev/meta/error": "r",
            "/devices/dev/controls/c": "7",
        },
    )
    foreign = await client.get_device("dev")
    await wait_until(lambda: foreign.error == ControlErrorFlags.READ)
    await driver.close()
    await restart_broker(broker, client)

    device = await client.create_device("dev")
    level = await device.create_control("c", ControlType.VALUE)

    assert device.error == ControlErrorFlags.NONE and level.value is None
    await device.set_error(ControlErrorFlags.READ)
    await level.set_value(7)
    await wait_retained(
        broker.port,
        "/devices/dev/#",
        {
            "/devices/dev/meta": b'{"driver": "test-service"}',
            "/devices/dev/meta/error": b"r",
            "/devices/dev/controls/c/meta": FIRST_VALUE_META,
            "/devices/dev/controls/c": b"7",
        },
    )


@pytest.mark.parametrize(
    "leftovers",
    [
        {"/devices/X/controls/a": "1", "/devices/X/controls/a/meta/error": "r"},
        {"/devices/X/meta/name": "Old", "/devices/X/controls/a/meta/type": "value"},
        {"/devices/X/meta": "{broken", "/devices/X/controls/a": "1"},
        {
            "/devices/X/meta/driver": "service",
            "/devices/X/meta/name": "Old",
            "/devices/X/controls/a/meta/type": "value",
            "/devices/X/controls/a": "1",
        },
        {
            "/devices/X/meta": '{"title": {"en": "Old"}}',
            "/devices/X/meta/driver": "service",
            "/devices/X/controls/a": "1",
        },
    ],
    ids=[
        "values without meta",
        "legacy subtopics without a driver",
        "broken JSON meta",
        "legacy /meta/driver of the service",
        "JSON meta without driver over /meta/driver of the service",
    ],
)
async def test_create_device_erases_leftovers_without_force(
    broker: MosquittoBroker, connect: ConnectClient, leftovers: dict[str, str]
) -> None:
    """
    Topics under the name that make no device, or a device of the own driver, are leftovers. create_device
    erases them without force. An older version of a service kept its driver only in /meta/driver, it
    counts when the JSON meta names no driver.
    """
    old = await connect("old-version")
    await publish_retained(old, leftovers)
    await old.close()
    client = await connect("service")

    await client.create_device("X")

    await wait_retained(broker.port, "/devices/X/#", {"/devices/X/meta": b'{"driver": "service"}'})


async def test_remove_driver_devices_finds_legacy_signatures_and_names_what_it_erased(
    broker: MosquittoBroker, connect: ConnectClient
) -> None:
    """
    remove_driver_devices erases the devices created now and the leftovers of previous runs, signed by
    the JSON meta or only by /meta/driver, and returns their names. A device of another driver stays,
    also with a stale /meta/driver of the service under its JSON meta. A JSON meta with a text that is not
    valid in UTF-8 signs its device as a live reader reads it.
    """
    old = await connect("old-version")
    await publish_retained(
        old,
        {
            "/devices/leftover/meta": '{"driver": "service"}',
            "/devices/leftover/controls/old/meta": '{"type": "value"}',
            "/devices/leftover/controls/old": "1",
            "/devices/legacy/meta/driver": "service",
            "/devices/legacy/meta/name": "Legacy",
            "/devices/legacy/controls/a/meta/type": "value",
            "/devices/legacy/controls/a": "1",
            "/devices/untitled/meta": '{"title": {"en": "Untitled"}}',
            "/devices/untitled/meta/driver": "service",
            "/devices/foreign/meta/driver": "other-driver",
            "/devices/foreign/controls/a": "1",
            "/devices/stale/meta": '{"driver": "other-driver"}',
            "/devices/stale/meta/driver": "service",
        },
    )
    await old.raw.publish("/devices/garbled/meta", b'{"driver": "service", "title": "\xff"}', retain=True)
    await old.close()
    client = await connect("service")
    await (await client.create_device("current")).create_control("c", ControlType.VALUE, value=1)

    assert await client.remove_driver_devices() == ["current", "garbled", "leftover", "legacy", "untitled"]

    remaining = await read_retained(broker.port, "/devices/#")
    assert sorted({topic.split("/")[2] for topic in remaining}) == ["foreign", "stale"]
