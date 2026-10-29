"""
Own devices: what the service publishes, commands from /on, removal and cleanup by the driver name.
"""

import asyncio
import json
from typing import Any

import pytest

from tests.broker import (
    FIRST_VALUE_META,
    INVALID_NAMES,
    DeadLinkProxy,
    MosquittoBroker,
    Send,
    cancel_and_wait,
    control_object,
    device_object,
    intercept_publish,
    internals,
    publish_retained,
    read_retained,
    restart_broker,
    wait_or_dump,
    wait_retained,
    wait_until,
    window_of_controls,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import (
    AlreadyCreatedError,
    BrokerDisconnectedError,
    Client,
    Control,
    ControlErrorFlags,
    ControlType,
    DeviceTakenError,
    InvalidNameError,
    Message,
    MqttLibError,
    NotOwnError,
    TranslatedTitle,
)
from wb_mqtt_lib._mqtt import broker_connection
from wb_mqtt_lib._mqtt import subscriber as subscriber_module

# What remove() erases under a device with raw subtopics, in this order, and what stays in the broker
RAW_SUBTOPIC_ERASURES = {
    "device": (
        [
            "/devices/dev/controls/temp",
            "/devices/dev/controls/temp/meta/error",
            "/devices/dev/controls/temp/meta/type",
            "/devices/dev/controls/temp/meta",
            "/devices/dev/meta/name",
            "/devices/dev/meta",
            "/devices/dev/meta/driver",
        ],
        {},
    ),
    "control": (
        [
            "/devices/dev/controls/temp",
            "/devices/dev/controls/temp/meta/error",
            "/devices/dev/controls/temp/meta/type",
            "/devices/dev/controls/temp/meta",
        ],
        {
            "/devices/dev/meta": b'{"driver": "test-service"}',
            "/devices/dev/meta/name": b"Dev",
            "/devices/dev/meta/driver": b"test-service",
        },
    ),
}


async def watch_erasures(connect: ConnectClient, topic_filter: str) -> list[str]:
    """
    Topics erased under the filter from now on, in the order the broker sends the erasures.
    """
    erased: list[str] = []

    def on_message(message: Message) -> None:
        if not message.payload:
            erased.append(message.topic)

    await (await connect("watcher")).raw.subscribe(topic_filter, on_message)
    return erased


async def test_device_meta_and_control_order_follow_convention(
    broker: MosquittoBroker, client: Client
) -> None:
    """
    The device meta carries the driver and both title languages. Order goes by creation and on from
    an explicit one.
    """
    device = await client.create_device("test_dev", title={"en": "Test", "ru": "Тест"})
    await device.create_control("temp", ControlType.VALUE)
    await device.create_control("relay", ControlType.SWITCH)
    await device.create_control("level", ControlType.RANGE, order=10)
    await device.create_control("next", ControlType.VALUE)

    retained = await read_retained(broker.port, "/devices/test_dev/#")

    assert json.loads(retained["/devices/test_dev/meta"]) == {
        "driver": "test-service",
        "title": {"en": "Test", "ru": "Тест"},
    }
    orders = {
        name: json.loads(retained[f"/devices/test_dev/controls/{name}/meta"])["order"]
        for name in ("temp", "relay", "level", "next")
    }
    assert orders == {"temp": 1, "relay": 2, "level": 10, "next": 11}


@pytest.mark.parametrize(
    "declaration, meta",
    [
        (
            {"control_type": ControlType.VALUE, "title": "Temperature", "units": "deg C", "precision": 0.1},
            {"type": "value", "title": {"en": "Temperature"}, "units": "deg C", "precision": 0.1},
        ),
        (
            {
                "control_type": ControlType.VALUE,
                "hidden": True,
                "enum": {"0": {"en": "Off", "ru": "Выкл"}, "1": "On"},
            },
            {"type": "value", "hidden": True, "enum": {"0": {"en": "Off", "ru": "Выкл"}, "1": {"en": "On"}}},
        ),
        (
            {"control_type": ControlType.RANGE, "minimum": 0, "maximum": 1000, "readonly": True},
            {"type": "range", "readonly": True, "min": 0, "max": 1000},
        ),
        (
            {"control_type": ControlType.VALUE, "title": TranslatedTitle("Level", "Уровень")},
            {"type": "value", "title": {"en": "Level", "ru": "Уровень"}},
        ),
    ],
    ids=["title, units and precision", "hidden and enum", "range limits and readonly", "title object"],
)
async def test_control_meta_goes_out_by_convention_and_comes_back(
    broker: MosquittoBroker, driver: Client, client: Client, declaration: dict, meta: dict
) -> None:
    """
    The declaration becomes the JSON meta of the convention, a reader gets the same meta back.
    """
    own = await (await driver.create_device("dev")).create_control("ctl", **declaration)

    retained = await read_retained(broker.port, "/devices/dev/controls/ctl/meta")
    remote = await client.get_control("dev/ctl")

    assert json.loads(retained["/devices/dev/controls/ctl/meta"]) == {
        "readonly": False,
        "hidden": False,
        "order": 1,
        **meta,
    }
    assert remote.meta == own.meta


async def test_update_meta_changes_fields_on_the_fly(
    broker: MosquittoBroker, driver: Client, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Title, readonly and hidden change without recreating the control, the value stays, readers see on_meta.

    hidden is always in the meta, so hidden=False replaces True in an open web interface. A field the meta
    cannot carry is refused before anything changes, the type too, it stays as created. The same fields
    once more publish nothing. A field given None leaves the meta.
    """
    device = await driver.create_device("test_dev", title="Old")
    temp = await device.create_control("temp", ControlType.VALUE, title="Temp", hidden=True, value=1)
    remote = await client.get_control("test_dev/temp")
    remote_device = await client.get_device("test_dev")
    metas = []
    remote.on_meta(
        lambda control: metas.append((control.meta.title, control.meta.readonly, control.meta.hidden))
    )

    await temp.update_meta(title={"en": "Temperature", "ru": "Температура"}, readonly=True, hidden=False)
    await device.update_meta(title="New")

    await wait_until(lambda: len(metas) == 1 and remote_device.meta.title.en == "New")
    assert metas == [(TranslatedTitle("Temperature", "Температура"), True, False)]
    retained = await read_retained(broker.port, "/devices/test_dev/#")
    assert json.loads(retained["/devices/test_dev/controls/temp/meta"]) == {
        "type": "value",
        "readonly": True,
        "hidden": False,
        "title": {"en": "Temperature", "ru": "Температура"},
        "order": 1,
    }
    assert json.loads(retained["/devices/test_dev/meta"]) == {
        "driver": "driver",
        "title": {"en": "New"},
    }
    assert retained["/devices/test_dev/controls/temp"] == b"1"
    meta = temp.meta
    for fields, error in [
        ({"colour": "red"}, TypeError),
        ({"control_type": ControlType.SWITCH}, TypeError),
        ({"precision": "0.1"}, TypeError),
        ({"precision": 0}, ValueError),
        ({"maximum": float("inf")}, ValueError),
        ({"order": float("nan")}, TypeError),
        ({"readonly": None}, TypeError),
        ({"title": {"en": "\ud800"}}, TypeError),
    ]:
        with pytest.raises(error):
            await temp.update_meta(**fields)
    assert temp.meta == meta
    sent: list[str] = []

    async def record(send: Send, topic: str, _payload: Any) -> None:
        if topic.startswith("/devices/"):
            sent.append(topic)
        await send()

    intercept_publish(monkeypatch, record)
    await temp.update_meta(title={"en": "Temperature", "ru": "Температура"}, readonly=True, hidden=False)
    await device.update_meta(title="New")
    assert not sent

    await temp.update_meta(title=None)
    retained = await read_retained(broker.port, "/devices/test_dev/controls/temp/meta")
    assert "title" not in json.loads(retained["/devices/test_dev/controls/temp/meta"])


@pytest.mark.parametrize("sent", ["before the device", "while the device listens"])
async def test_retained_command_is_erased_and_live_command_reaches_handler(
    broker: MosquittoBroker, connect: ConnectClient, client: Client, sent: str
) -> None:
    """
    A retained command in /on is not executed, one left from the past and one published while the client
    listens. An empty live one does not reach the handler.

    A live one goes to on_command with a typed value.
    """
    other = await connect("other")
    if sent == "before the device":
        await other.raw.publish("/devices/test_dev/controls/relay/on", "1", retain=True)
    device = await client.create_device("test_dev")
    relay = await device.create_control("relay", ControlType.SWITCH, value=False)
    commands = []
    relay.on_command(lambda control, value: commands.append(value))
    if sent == "while the device listens":
        await other.raw.publish("/devices/test_dev/controls/relay/on", "1", retain=True)

    await wait_retained(broker.port, "/devices/test_dev/controls/relay/on", {})
    assert not commands

    await other.raw.publish("/devices/test_dev/controls/relay/on", "")
    await other.raw.publish("/devices/test_dev/controls/relay/on", "2")
    await wait_until(lambda: commands == [True])


async def test_command_to_an_own_pushbutton_comes_as_true(connect: ConnectClient, client: Client) -> None:
    """
    A command to an own pushbutton reaches on_command and on_control_command as True.
    """
    device = await client.create_device("dev")
    button = await device.create_control("button", ControlType.PUSHBUTTON)
    commands: list[tuple[str, Any]] = []
    button.on_command(lambda _control, value: commands.append(("control", value)))
    device.on_control_command(lambda _control, value: commands.append(("device", value)))
    user = await connect("user")

    await user.raw.publish("/devices/dev/controls/button/on", "1")

    await wait_until(lambda: len(commands) == 2)
    assert sorted(commands) == [("control", True), ("device", True)]


@pytest.mark.parametrize("offline", [False, True], ids=["online", "offline"])
async def test_remove_erases_controls_first_and_device_meta_last(
    broker: MosquittoBroker, connect: ConnectClient, dead_link: tuple[DeadLinkProxy, Client], offline: bool
) -> None:
    """
    remove() erases in three stages, each after the broker confirmed the one before: values and errors
    of the controls in reverse order of creation, then their meta, then the device meta. A half done
    removal leaves the meta with the driver name, so cleanup finds it later. Without a link the erasures
    go out after reconnect in the same order.
    """
    proxy, client = dead_link
    device = await client.create_device("dev")
    first = await device.create_control("a", ControlType.VALUE, value=1)
    await device.create_control("b", ControlType.VALUE, value=2)
    erased = await watch_erasures(connect, "/devices/dev/#")
    if offline:
        await proxy.drop(client)

    await device.remove()

    assert not device.exists and not first.exists
    await proxy.restore(client)
    await wait_retained(broker.port, "/devices/dev/#", {})
    await wait_until(lambda: len(erased) == 7)
    assert erased == [
        "/devices/dev/controls/b",
        "/devices/dev/controls/b/meta/error",
        "/devices/dev/controls/a",
        "/devices/dev/controls/a/meta/error",
        "/devices/dev/controls/b/meta",
        "/devices/dev/controls/a/meta",
        "/devices/dev/meta",
    ]


@pytest.mark.parametrize("offline", [False, True], ids=["online", "offline"])
async def test_erasure_stage_waits_until_the_broker_confirmed_the_one_before(
    broker: MosquittoBroker,
    dead_link: tuple[DeadLinkProxy, Client],
    monkeypatch: pytest.MonkeyPatch,
    offline: bool,
) -> None:
    """
    The broker confirms one value erasure of remove() late. Until then the meta of the controls
    and of the device stays in the broker, the next stages wait. A removal made without a link keeps
    its stages when it goes out after reconnect.
    """
    proxy, client = dead_link
    device = await client.create_device("dev")
    for name in ("a", "b"):
        await device.create_control(name, ControlType.VALUE, value=1)
    held = asyncio.Event()
    release = asyncio.Event()

    async def confirm_the_first_erasure_late(send: Send, _topic: str, payload: Any) -> None:
        await send()
        if payload is None and not held.is_set():
            held.set()
            await release.wait()

    intercept_publish(monkeypatch, confirm_the_first_erasure_late)
    if offline:
        await proxy.drop(client)
    removal = asyncio.ensure_future(device.remove())
    proxy.dead = False
    await wait_or_dump(held.wait(), timeout=20)
    await asyncio.sleep(0.2)

    assert sorted(await read_retained(broker.port, "/devices/dev/#")) == [
        "/devices/dev/controls/a/meta",
        "/devices/dev/controls/b/meta",
        "/devices/dev/meta",
    ]
    release.set()
    await wait_or_dump(removal, timeout=5)
    await wait_retained(broker.port, "/devices/dev/#", {})


@pytest.mark.parametrize("name", INVALID_NAMES)
async def test_invalid_name_is_refused_before_the_broker_hears_of_it(
    broker: MosquittoBroker, client: Client, name: str
) -> None:
    """
    An empty name or /, +, # would break the topic structure, a control character would make mosquitto drop
    the connection. Every call that takes a device, control or driver name refuses it before it subscribes
    or publishes anything.
    """
    device = await client.create_device("dev")
    subscriptions = internals(client)["subscriptions"]
    calls = [
        client.create_device(name),
        device.create_control(name, ControlType.VALUE),
        client.get_device(name),
        client.get_control(f"{name}/ctl"),
        client.get_control(f"dev/{name}"),
        client.get_device_snapshot(name),
    ]

    for call in calls:
        with pytest.raises(InvalidNameError):
            await call
    with pytest.raises(InvalidNameError) as error:
        Client(name)
    assert isinstance(error.value, MqttLibError) and isinstance(error.value, ValueError)

    assert internals(client)["subscriptions"] == subscriptions
    assert list(await read_retained(broker.port, "#")) == ["/devices/dev/meta"]


async def test_create_device_returns_the_device_when_the_link_drops_at_its_command_subscription(
    connect: ConnectClient, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    The link drops while create_device subscribes to the commands of the new device. The device is own already
    and the subscription stays in the client until reconnect, so create_device returns the device.
    """
    subscribe = subscriber_module.Subscriber.subscribe
    cut: list[str] = []

    async def cut_the_command_subscription(
        self: subscriber_module.Subscriber, topic_filters: list[str], *args: Any, **kwargs: Any
    ) -> None:
        await subscribe(self, topic_filters, *args, **kwargs)
        if topic_filters[0].endswith("/on") and not cut:
            cut.append(topic_filters[0])
            raise BrokerDisconnectedError("the broker link is lost")

    monkeypatch.setattr(subscriber_module.Subscriber, "subscribe", cut_the_command_subscription)

    device = await wait_or_dump(client.create_device("dev"), timeout=5)

    assert device.is_own and cut == ["/devices/dev/controls/+/on"]
    relay = await device.create_control("relay", ControlType.SWITCH, value=False)
    commands: list[bool] = []
    relay.on_command(lambda _control, value: commands.append(value))
    user = await connect("user")
    await user.raw.publish("/devices/dev/controls/relay/on", "1")
    await wait_until(lambda: commands == [True])


async def test_command_to_readonly_control_reaches_no_handler_until_readonly_is_off(
    connect: ConnectClient, client: Client
) -> None:
    """
    A readonly control has no /on by the convention. Like wb-rules, the client drops a command to it, for the
    control handler and the device handler alike. Once readonly is off, commands come again.
    """
    device = await client.create_device("dev")
    relay = await device.create_control("relay", ControlType.SWITCH, value=False, readonly=True)
    commands: list[str] = []
    relay.on_command(lambda _control, value: commands.append(f"control {value}"))
    device.on_control_command(lambda _control, value: commands.append(f"device {value}"))
    # The raw handler gets the command after the client has already passed it on or dropped it
    seen: list[Message] = []
    await client.raw.subscribe("/devices/dev/controls/relay/on", seen.append)
    user = await connect("user")

    await user.raw.publish("/devices/dev/controls/relay/on", "1")
    await wait_until(lambda: len(seen) == 1)
    await relay.update_meta(readonly=False)
    await user.raw.publish("/devices/dev/controls/relay/on", "0")

    await wait_until(lambda: sorted(commands) == ["control False", "device False"])


async def test_device_and_control_exist_once(client: Client) -> None:
    """
    A second create of the same name is an AlreadyCreatedError, also two creates of one device at once.
    It is a MqttLibError and a ValueError too.
    """
    device = await client.create_device("X")
    await device.create_control("c", ControlType.VALUE, value=1)

    with pytest.raises(AlreadyCreatedError) as error:
        await client.create_device("X")
    assert not isinstance(error.value, DeviceTakenError)
    assert isinstance(error.value, MqttLibError) and isinstance(error.value, ValueError)
    with pytest.raises(AlreadyCreatedError):
        await device.create_control("c", ControlType.VALUE)
    subscriptions = internals(client)["subscriptions"]
    results = await asyncio.gather(
        client.create_device("Y"), client.create_device("Y"), return_exceptions=True
    )

    assert sorted(type(result).__name__ for result in results) == ["AlreadyCreatedError", "Device"]
    # the commands to the controls of Y, an own device is not followed otherwise
    assert internals(client)["subscriptions"] == subscriptions + 1


@pytest.mark.parametrize("other", ["remove", "remove with a handler", "set_value"])
async def test_create_control_racing_another_task_leaves_a_consistent_state(
    broker: MosquittoBroker, client: Client, other: str
) -> None:
    """
    Another task removes the device or writes the control while create_control waits for the broker.

    create_control does not fail, a removed control leaves nothing in the broker, a newer value stays.
    """
    device = await client.create_device("dev")
    if other == "remove with a handler":
        control_object(client, "dev/c").on_value(lambda control, value, previous: None)

    async def other_task() -> None:
        if other == "set_value":
            await control_object(client, "dev/c").set_value(7)
        else:
            await device.remove()

    await wait_or_dump(
        asyncio.gather(device.create_control("c", ControlType.VALUE, value=5), other_task()), timeout=5
    )

    expected = {}
    if other == "set_value":
        expected = {
            "/devices/dev/meta": b'{"driver": "test-service"}',
            "/devices/dev/controls/c/meta": FIRST_VALUE_META,
            "/devices/dev/controls/c": b"7",
        }
    await wait_retained(broker.port, "/devices/dev/#", expected)
    assert control_object(client, "dev/c").value == (7 if other == "set_value" else None)
    await restart_broker(broker, client)
    await wait_retained(broker.port, "/devices/dev/#", expected)


async def test_own_pushbutton_press_is_an_event_only_when_it_went_out(
    broker: MosquittoBroker, client: Client
) -> None:
    """
    Every press reaches on_value as (True, None). Without a link a press raises and is not an event.
    """
    button = await (await client.create_device("dev")).create_control("button", ControlType.PUSHBUTTON)
    presses = []
    button.on_value(lambda control, value, previous: presses.append((value, previous)))

    await button.set_value(True)
    await button.set_value(True)
    await wait_until(lambda: len(presses) == 2)
    await asyncio.to_thread(broker.stop)
    await wait_until(lambda: not client.is_connected)
    with pytest.raises(BrokerDisconnectedError):
        await button.set_value(True)
    await asyncio.sleep(0.2)

    assert presses == [(True, None), (True, None)] and button.value is None


async def test_own_listeners_survive_removal_and_creation_again(
    connect: ConnectClient, client: Client
) -> None:
    """
    Listeners of an own control stay through remove() and work with the control created again.
    """
    device = await client.create_device("dev")
    relay = await device.create_control("relay", ControlType.SWITCH, value=False)
    events: dict[str, list] = {"added": [], "removed": [], "value": [], "command": []}
    relay.on_added(events["added"].append)
    relay.on_removed(events["removed"].append)
    relay.on_value(lambda control, value, previous: events["value"].append((value, previous)))
    relay.on_command(lambda control, value: events["command"].append(value))
    await wait_until(lambda: events["added"] and events["value"])

    await device.remove()
    device = await client.create_device("dev")
    await device.create_control("relay", ControlType.SWITCH, value=True)
    user = await connect("user")
    await user.raw.publish("/devices/dev/controls/relay/on", "0")

    await wait_until(lambda: events["command"] == [False])
    assert events["added"] == [relay, relay] and events["removed"] == [relay]
    assert events["value"] == [(False, None), (True, None)]


async def test_remove_driver_devices_erases_controls_first_and_device_meta_last(
    connect: ConnectClient,
) -> None:
    """
    A half done cleanup leaves the device meta with the driver name, so the next one finds it.
    """
    other = await connect("other")
    await publish_retained(
        other,
        {
            "/devices/old/meta": '{"driver": "service"}',
            "/devices/old/meta/name": "Old",
            "/devices/old/controls/a/meta": '{"type": "value"}',
            "/devices/old/controls/a": "1",
            "/devices/old/controls/a/meta/error": "r",
        },
    )
    erased = await watch_erasures(connect, "/devices/old/#")
    client = await connect("service")

    await client.remove_driver_devices()

    await wait_until(lambda: len(erased) == 5)
    assert erased == [
        "/devices/old/controls/a",
        "/devices/old/controls/a/meta/error",
        "/devices/old/controls/a/meta",
        "/devices/old/meta/name",
        "/devices/old/meta",
    ]


async def test_remove_driver_devices_right_after_reconnect_erases_own_devices_not_replayed_yet(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    remove_driver_devices() is called right after reconnect, before the replay brought the own devices back
    to the broker. It erases them all the same, the client knows its own devices.
    """
    names = [f"d{index:02d}" for index in range(5)]
    for name in names:
        await (await client.create_device(name)).create_control("c", ControlType.VALUE, value=1)
    release = asyncio.Event()

    async def hold_device_metas(send: Send, topic: str, payload: Any) -> None:
        if payload is not None and topic.count("/") == 3 and topic.endswith("/meta"):
            await release.wait()
        await send()

    intercept_publish(monkeypatch, hold_device_metas)
    await restart_broker(broker, client)
    # The erasures wait behind the held metas, those go out once remove_driver_devices has read the broker
    asyncio.get_running_loop().call_later(1, release.set)

    removed = await wait_or_dump(client.remove_driver_devices(), timeout=10)

    assert removed == names and not any(device_object(client, name).is_own for name in names)
    await wait_retained(broker.port, "/devices/#", {})


async def test_remove_driver_devices_erases_a_device_that_a_cancelled_create_device_finishes(
    broker: MosquittoBroker, connect: ConnectClient, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A service stops while create_device erases what its previous run left under the name, as in the quick
    start. The cancelled create_device still makes the device own, remove_driver_devices right after it
    waits for that and erases the device too.
    """
    other = await connect("other")
    await publish_retained(
        other,
        {
            "/devices/dev/meta": '{"driver": "test-service"}',
            "/devices/dev/controls/old/meta": '{"type": "value"}',
            "/devices/dev/controls/old": "1",
        },
    )
    erasing = asyncio.Event()

    async def slow_erasures(send: Send, topic: str, payload: Any) -> None:
        if payload is None and topic.startswith("/devices/dev/"):
            erasing.set()
            await asyncio.sleep(0.3)
        await send()

    intercept_publish(monkeypatch, slow_erasures)
    creating = asyncio.ensure_future(client.create_device("dev"))
    await wait_or_dump(erasing.wait(), timeout=5)
    await cancel_and_wait(creating)

    assert await wait_or_dump(client.remove_driver_devices(), timeout=10) == ["dev"]
    await wait_until(lambda: internals(client)["internal tasks"] == 0)
    assert not device_object(client, "dev").is_own
    await wait_retained(broker.port, "/devices/dev/#", {})


async def test_device_error_is_set_cleared_and_erased_before_the_device_meta(
    broker: MosquittoBroker, connect: ConnectClient, client: Client
) -> None:
    """
    The error of the device itself goes to /devices/<device>/meta/error, once per change. remove() erases it
    before the device meta.
    """
    device = await client.create_device("dev")
    await device.create_control("temp", ControlType.VALUE, value=1)
    events = []
    device.on_error(lambda _device, error, previous: events.append((error, previous)))
    watcher = await connect("watcher")
    seen: list[tuple[str, bytes]] = []
    await watcher.raw.subscribe(
        "/devices/dev/#", lambda message: seen.append((message.topic, message.payload))
    )

    await device.set_error(ControlErrorFlags.READ)
    await device.set_error(ControlErrorFlags.READ)
    await device.set_error(ControlErrorFlags.NONE)
    await device.set_error(ControlErrorFlags.WRITE)
    assert device.error == ControlErrorFlags.WRITE
    await device.remove()

    await wait_until(lambda: ("/devices/dev/meta", b"") in seen)
    errors = [payload for topic, payload in seen if topic == "/devices/dev/meta/error"]
    assert errors == [b"r", b"", b"w", b""]
    last_seen = {topic: index for index, (topic, _payload) in enumerate(seen)}
    assert last_seen["/devices/dev/meta/error"] < last_seen["/devices/dev/meta"]
    assert await read_retained(broker.port, "/devices/dev/#") == {}
    await wait_until(lambda: len(events) == 4)
    assert events == [
        (ControlErrorFlags.NONE, None),
        (ControlErrorFlags.READ, ControlErrorFlags.NONE),
        (ControlErrorFlags.NONE, ControlErrorFlags.READ),
        (ControlErrorFlags.WRITE, ControlErrorFlags.NONE),
    ]


@pytest.mark.parametrize("steps", [0, 1, 2, 5])
async def test_create_control_cancelled_at_any_point_is_whole_or_absent(
    broker: MosquittoBroker, client: Client, steps: int
) -> None:
    """
    The task that creates a control is cancelled while it waits for the broker. The control is either
    there whole, with its meta and value in the broker also after reconnect, or not there at all.
    """
    device = await client.create_device("dev")
    attempt = asyncio.ensure_future(device.create_control("c", ControlType.VALUE, value=5))
    for _ in range(steps):
        await asyncio.sleep(0)
    await cancel_and_wait(attempt)
    control = control_object(client, "dev/c")
    expected = {}
    if control.exists:
        expected = {"/devices/dev/controls/c/meta": FIRST_VALUE_META, "/devices/dev/controls/c": b"5"}

    await wait_retained(broker.port, "/devices/dev/controls/c/#", expected)
    await restart_broker(broker, client)
    await wait_retained(broker.port, "/devices/dev/controls/c/#", expected)


@pytest.mark.parametrize("steps", [1, 2, 5])
async def test_removal_cancelled_midway_still_erases_the_control(
    broker: MosquittoBroker, client: Client, steps: int
) -> None:
    """
    The task that removes a control is cancelled while the erasure goes out. The control is gone in the
    broker and stays gone after reconnect, a second remove() has nothing to do.
    """
    control = await (await client.create_device("dev")).create_control("c", ControlType.VALUE, value=5)
    await control.set_error(ControlErrorFlags.READ)
    attempt = asyncio.ensure_future(control.remove())
    for _ in range(steps):
        await asyncio.sleep(0)
    await cancel_and_wait(attempt)

    assert not control.exists
    await wait_retained(broker.port, "/devices/dev/controls/c/#", {})
    await restart_broker(broker, client)
    await wait_retained(broker.port, "/devices/dev/controls/c/#", {})
    await control.remove()


async def test_control_created_again_while_its_removal_goes_out_keeps_the_new_meta(
    broker: MosquittoBroker, client: Client
) -> None:
    """
    One task removes a control, another creates it again with another type before the erasure is through.
    The erasure does not touch the new control, in the broker and after reconnect.
    """
    device = await client.create_device("dev")
    mode = await device.create_control("mode", ControlType.VALUE, value=1)

    await wait_or_dump(
        asyncio.gather(mode.remove(), device.create_control("mode", ControlType.TEXT, value="auto")),
        timeout=5,
    )

    meta = b'{"type": "text", "readonly": false, "hidden": false, "order": 2}'
    expected = {"/devices/dev/controls/mode/meta": meta, "/devices/dev/controls/mode": b"auto"}
    assert mode.exists and mode.value == "auto"
    await wait_retained(broker.port, "/devices/dev/controls/mode/#", expected)
    await restart_broker(broker, client)
    await wait_retained(broker.port, "/devices/dev/controls/mode/#", expected)


async def test_device_created_again_while_its_removal_goes_out_gets_commands(
    connect: ConnectClient, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    One task removes a device, another creates it again while the broker has not confirmed the erasure
    of the old device meta yet. The new device keeps the subscription to its commands.
    """
    device = await client.create_device("dev")
    await device.create_control("relay", ControlType.SWITCH, value=False)
    commands: list[bool] = []
    delayed: list[str] = []

    async def confirm_the_first_meta_erasure_late(send: Send, topic: str, payload: Any) -> None:
        await send()
        if topic == "/devices/dev/meta" and payload is None and not delayed:
            delayed.append(topic)
            await asyncio.sleep(1)

    intercept_publish(monkeypatch, confirm_the_first_meta_erasure_late)

    async def create_again() -> None:
        again = await client.create_device("dev")
        relay = await again.create_control("relay", ControlType.SWITCH, value=False)
        relay.on_command(lambda _control, value: commands.append(value))

    await wait_or_dump(asyncio.gather(device.remove(), create_again()), timeout=5)
    user = await connect("user")
    await user.raw.publish("/devices/dev/controls/relay/on", "1")

    await wait_until(lambda: commands == [True])
    assert delayed and internals(client)["subscriptions"] == 1


@pytest.mark.parametrize("with_handler", [False, True], ids=["without a handler", "with a handler"])
async def test_press_of_a_button_removed_before_the_broker_answered_is_no_event(
    client: Client, with_handler: bool
) -> None:
    """
    A press waits for PUBACK, and the button is removed meanwhile. The press ends without an error
    and without an event.
    """
    button = await (await client.create_device("dev")).create_control("button", ControlType.PUSHBUTTON)
    presses: list = []
    if with_handler:
        button.on_value(lambda _control, value, _previous: presses.append(value))

    press = asyncio.ensure_future(button.set_value(True))
    await asyncio.sleep(0)
    await button.remove()
    await wait_or_dump(press, timeout=5)

    await asyncio.sleep(0.1)
    assert not presses and not button.exists


async def test_changes_reach_the_broker_in_the_order_of_the_calls(
    connect: ConnectClient, client: Client
) -> None:
    """
    Changes of several tasks go out in the order of the calls, each with its own value, also while
    the erasure of another device holds them between its stages. A press does not overtake a value set
    before it. A repeated value does not go out, every press does.
    """
    other = await client.create_device("other")
    for name in ("a", "b"):
        await other.create_control(name, ControlType.VALUE, value=1)
    device = await client.create_device("dev")
    level = await device.create_control("level", ControlType.VALUE, value=0)
    button = await device.create_control("button", ControlType.PUSHBUTTON)
    watcher = await connect("watcher")
    seen: list[str] = []

    def collect(message: Message) -> None:
        seen.append(f"{message.topic.rsplit('/', 1)[1]}={message.payload.decode()}")

    await watcher.raw.subscribe("/devices/dev/controls/+", collect)
    await wait_until(lambda: seen == ["level=0"])
    seen.clear()

    await wait_or_dump(
        asyncio.gather(
            other.remove(),
            level.set_value(1),
            level.set_value(2),
            level.set_value(2),
            button.set_value(True),
            button.set_value(True),
            level.set_value(3),
        ),
        timeout=5,
    )

    await wait_until(lambda: len(seen) == 5)
    assert seen == ["level=1", "level=2", "button=1", "button=1", "level=3"]


@pytest.mark.parametrize("offline", [False, True], ids=["online", "offline"])
@pytest.mark.parametrize("removed", RAW_SUBTOPIC_ERASURES)
async def test_remove_erases_what_the_service_published_under_it(
    broker: MosquittoBroker,
    connect: ConnectClient,
    dead_link: tuple[DeadLinkProxy, Client],
    offline: bool,
    removed: str,
) -> None:
    """
    remove() of a device or a control erases also the legacy meta subtopics the service published itself
    with raw.publish, without a link after reconnect. A subtopic goes before the meta it belongs to, one
    of a control with the values, one of the device with the control meta, /meta/driver with the device meta.
    """
    erased, remaining = RAW_SUBTOPIC_ERASURES[removed]
    proxy, client = dead_link
    device = await client.create_device("dev")
    temp = await device.create_control("temp", ControlType.VALUE, value=1)
    await client.raw.publish("/devices/dev/meta/name", "Dev", retain=True)
    await client.raw.publish("/devices/dev/meta/driver", "test-service", retain=True)
    await client.raw.publish("/devices/dev/controls/temp/meta/type", "value", retain=True)
    seen = await watch_erasures(connect, "/devices/dev/#")
    if offline:
        await proxy.drop(client)

    await (device if removed == "device" else temp).remove()

    await proxy.restore(client)
    await wait_retained(broker.port, "/devices/dev/#", remaining)
    await wait_until(lambda: len(seen) == len(erased))
    assert seen == erased


async def test_device_handler_gets_commands_of_all_controls_in_order(
    connect: ConnectClient, driver: Client, client: Client
) -> None:
    """
    on_control_command of an own device gets the commands of every control in one queue, in the order
    they came. remove_handler of a control does not take it off, the one of the device does. A foreign
    device refuses it.
    """
    device = await client.create_device("dev")
    first = await device.create_control("K1", ControlType.SWITCH, value=False)
    await device.create_control("K2", ControlType.SWITCH, value=False)
    commands: list[tuple[str, Any]] = []

    async def on_command(control: Control, value: Any) -> None:
        commands.append((control.name, value))
        await asyncio.sleep(0.01)

    device.on_control_command(on_command)
    user = await connect("user")
    for name, payload in [("K1", "1"), ("K2", "1"), ("K1", "0"), ("K2", "0")]:
        await user.raw.publish(f"/devices/dev/controls/{name}/on", payload)
    await wait_until(lambda: len(commands) == 4)
    first.remove_handler(on_command)
    await user.raw.publish("/devices/dev/controls/K1/on", "1")
    await wait_until(lambda: len(commands) == 5)
    device.remove_handler(on_command)
    await user.raw.publish("/devices/dev/controls/K2/on", "1")

    await asyncio.sleep(0.2)
    assert commands == [("K1", True), ("K2", True), ("K1", False), ("K2", False), ("K1", True)]
    await (await driver.create_device("foreign")).create_control("K1", ControlType.SWITCH, value=False)
    with pytest.raises(NotOwnError):
        (await client.get_device("foreign")).on_control_command(on_command)


async def test_own_change_returns_before_puback_while_the_window_has_room(
    broker: MosquittoBroker, driver: Client, client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    set_value and set_error of an own device return once the connection took the message, the broker
    confirms it later. With the window full the next change waits for the broker, so a service cannot run
    ahead of it. update_meta and a command to a foreign control wait for PUBACK.
    """
    await (await driver.create_device("foreign")).create_control("level", ControlType.VALUE, value=0)
    remote = await client.get_control("foreign/level")
    controls = await window_of_controls(client)
    device = controls[0].device
    release = asyncio.Event()

    async def confirm_late(send: Send, _topic: str, _payload: Any) -> None:
        await send()
        await release.wait()

    intercept_publish(monkeypatch, confirm_late)
    command = asyncio.ensure_future(remote.set_value(5))
    meta = asyncio.ensure_future(controls[0].update_meta(units="V"))
    await asyncio.sleep(0.2)
    assert not command.done() and not meta.done()
    await wait_or_dump(controls[0].set_error(ControlErrorFlags.READ), timeout=1)
    await wait_or_dump(device.set_error(ControlErrorFlags.WRITE), timeout=1)
    # The command, the meta and two errors hold four places of the window, values fill the rest
    for control in controls[1 : broker_connection.SEND_WINDOW - 3]:
        await wait_or_dump(control.set_value(1), timeout=1)
    held = asyncio.ensure_future(controls[-1].set_value(1))
    await asyncio.sleep(0.2)
    assert not held.done()

    release.set()
    await wait_or_dump(asyncio.gather(command, meta, held), timeout=5)
    last = f"/devices/dev/controls/c{broker_connection.SEND_WINDOW - 1}"
    await wait_retained(broker.port, last, {last: b"1"})


async def test_own_change_right_before_close_reaches_the_broker(
    broker: MosquittoBroker, client: Client
) -> None:
    """
    A service sets a value and closes the client at once. The disconnect goes after the packet, so
    the value is in the broker although set_value did not wait for PUBACK.
    """
    temp = await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)

    await temp.set_value(2)
    await wait_or_dump(client.close(), timeout=5)

    assert await read_retained(broker.port, "/devices/dev/controls/temp") == {
        "/devices/dev/controls/temp": b"2"
    }
