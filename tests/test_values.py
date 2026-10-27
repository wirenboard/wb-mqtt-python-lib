"""
Values of own controls as they go into the broker, and declarations the library refuses.
"""

import json
from typing import Optional

import pytest

from tests.broker import (
    MosquittoBroker,
    publish_retained,
    read_retained,
    restart_broker,
    wait_retained,
    wait_until,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import Client, ControlErrorFlags, ControlType, Message, TranslatedTitle

# Numbers as wb-rules publishes them with precision and without it, checked on the controller
FORMATTED = [
    (ControlType.VALUE, 0.1, 21.53, b"21.5"),
    (ControlType.VALUE, 0.25, 20.1, b"20.1"),
    (ControlType.VALUE, 0.25, 20.25, b"20.25"),
    (ControlType.VALUE, 0.0625, 21.0625, b"21.0625"),
    (ControlType.RANGE, 0.5, 33.33, b"33.3"),
    (ControlType.VALUE, 1, 7.4, b"7"),
    (ControlType.VALUE, 0.01, 2.675, b"2.67"),
    (ControlType.VALUE, 0.1, 21, b"21"),
    (ControlType.VALUE, 0.1, -0.04, b"0"),
    (ControlType.VALUE, 0.001, 1e25, b"10000000000000000905969664"),
    (ControlType.VALUE, None, 7, b"7"),
    (ControlType.VALUE, None, 21.0, b"21"),
    (ControlType.VALUE, None, -0.0, b"0"),
    (ControlType.VALUE, None, 1e16, b"10000000000000000"),
    (ControlType.VALUE, None, 0.00001, b"0.00001"),
    (ControlType.VALUE, None, 0.1 + 0.2, b"0.30000000000000004"),
    (ControlType.RANGE, None, 100, b"100"),
    (ControlType.SWITCH, None, True, b"1"),
    (ControlType.SWITCH, None, False, b"0"),
    (ControlType.ALARM, None, True, b"1"),
    (ControlType.RGB, None, [255, 0, 10], b"255;0;10"),
    (ControlType.TEXT, None, 42, b"42"),
    (ControlType.TEXT, None, "hello", b"hello"),
    (ControlType.LOCAL_TIME, None, 1700000000, b"1700000000"),
    (ControlType.W1_ID, None, 2**63, b"9223372036854775808"),
]

REFUSED = [
    (ControlType.VALUE, True),
    (ControlType.VALUE, "5"),
    (ControlType.VALUE, float("nan")),
    (ControlType.VALUE, float("inf")),
    # An int too large for a float
    (ControlType.VALUE, 10**400),
    (ControlType.SWITCH, "0"),
    (ControlType.SWITCH, 0),
    (ControlType.SWITCH, 1),
    (ControlType.SWITCH, 1.0),
    (ControlType.SWITCH, 2),
    (ControlType.ALARM, 0),
    (ControlType.WO_SWITCH, 1),
    (ControlType.PUSHBUTTON, False),
    (ControlType.PUSHBUTTON, 1),
    (ControlType.PUSHBUTTON, None),
    (ControlType.RGB, (300, 0, 0)),
    (ControlType.RGB, "255;0;0"),
    (ControlType.RGB, (1, 2)),
    (ControlType.LOCAL_TIME, 1.5),
    (ControlType.W1_ID, True),
    (ControlType.TEXT, "a\udcff"),
]


async def test_value_goes_out_formatted_by_type(broker: MosquittoBroker, client: Client) -> None:
    """
    Precision rounds the MQTT payload, the own number stays as given. Without precision a number goes out
    in the shortest digits, without an exponent or trailing .0, as in wb-rules. Other types go out
    by convention.
    """
    device = await client.create_device("dev")
    controls = []
    for index, (control_type, precision, value, _) in enumerate(FORMATTED):
        controls.append(
            await device.create_control(f"c{index}", control_type, precision=precision, value=value)
        )

    retained = await read_retained(broker.port, "/devices/dev/controls/+")

    for index, (control_type, precision, value, payload) in enumerate(FORMATTED):
        assert retained[f"/devices/dev/controls/c{index}"] == payload, (control_type, precision, value)
        if control_type in (ControlType.VALUE, ControlType.RANGE):
            assert controls[index].value == value, (control_type, precision, value)
            assert type(controls[index].value) is type(value)
        elif control_type == ControlType.RGB:
            assert controls[index].value == tuple(value)
        elif control_type == ControlType.TEXT:
            assert controls[index].value == str(value)


async def test_value_of_another_type_is_refused_before_publication(
    broker: MosquittoBroker, connect: ConnectClient, client: Client
) -> None:
    """
    A value that does not fit the type raises TypeError, nothing changes in the broker or in the object.

    A command to a foreign control is checked the same way.
    """
    device = await client.create_device("dev")
    controls = [
        await device.create_control(f"c{index}", control_type)
        for index, (control_type, _) in enumerate(REFUSED)
    ]

    for control, (control_type, value) in zip(controls, REFUSED):
        with pytest.raises(TypeError):
            await control.set_value(value)
        assert control.value is None, (control_type, value)
    reader = await connect("reader")
    remote = await reader.get_control("dev/c0")
    with pytest.raises(TypeError):
        await remote.set_value("5")
    assert await read_retained(broker.port, "/devices/dev/controls/+") == {}


async def test_command_to_a_deprecated_number_type_is_a_checked_number(
    driver: Client, client: Client
) -> None:
    """
    A foreign control of a deprecated type derived from value, temperature for example, takes a command
    as a finite number, as value does, and the number goes out without an exponent.
    """
    await publish_retained(driver, {"/devices/dev/controls/t/meta": '{"type": "temperature"}'})
    commands: list[bytes] = []
    await driver.raw.subscribe("/devices/dev/controls/t/on", lambda message: commands.append(message.payload))
    remote = await client.get_control("dev/t")

    for value in ("abc", True, float("nan")):
        with pytest.raises(TypeError):
            await remote.set_value(value)
    await remote.set_value(0.00001)

    await wait_until(lambda: commands == [b"0.00001"])


async def test_invalid_declaration_publishes_nothing(broker: MosquittoBroker, client: Client) -> None:
    """
    A wrong type, a deprecated type, a wrong initial value or any initial value of a pushbutton, a meta field
    the meta cannot carry and a name too long for the topics of the control fail before the first
    publication. A lone surrogate has no UTF-8 form, a NaN order would make the meta invalid JSON.
    Of two wrong fields precision is reported first.

    The name stays free for a valid declaration, and its order is not taken.
    """
    device = await client.create_device("dev")
    calls = [
        (device.create_control("a", "swich"), ValueError),
        (device.create_control("a", "temperature"), ValueError),
        (device.create_control("a", ControlType.VALUE, value="abc"), TypeError),
        (device.create_control("a", ControlType.SWITCH, value=1), TypeError),
        (device.create_control("a", ControlType.RGB, value=(1, 2)), TypeError),
        (device.create_control("a", ControlType.PUSHBUTTON, value=True), TypeError),
        (device.create_control("a", ControlType.PUSHBUTTON, value=False), TypeError),
        (device.create_control("a", ControlType.PUSHBUTTON, value="x"), TypeError),
        (device.create_control("a", ControlType.VALUE, title=5), TypeError),
        (device.create_control("a", ControlType.VALUE, title={"en": 5}), TypeError),
        (device.create_control("a", ControlType.VALUE, enum=["on", "off"]), TypeError),
        (device.create_control("a", ControlType.VALUE, enum={"0": {"en": 0}}), TypeError),
        (device.create_control("a", ControlType.VALUE, precision="abc"), TypeError),
        (device.create_control("a", ControlType.VALUE, precision=-0.1), ValueError),
        (device.create_control("a", ControlType.VALUE, precision=float("inf")), ValueError),
        (device.create_control("a", ControlType.VALUE, precision=10**400), ValueError),
        (device.create_control("a", ControlType.VALUE, precision=0, minimum="1"), ValueError),
        (device.create_control("a", ControlType.RANGE, minimum="0"), TypeError),
        (device.create_control("a", ControlType.RANGE, minimum=float("-inf")), ValueError),
        (device.create_control("a", ControlType.RANGE, maximum=float("nan")), ValueError),
        (device.create_control("a", ControlType.VALUE, units=5), TypeError),
        (device.create_control("a", ControlType.VALUE, units="\ud800"), TypeError),
        (device.create_control("a", ControlType.VALUE, title="\ud800"), TypeError),
        (device.create_control("a", ControlType.VALUE, enum={"\ud800": "x"}), TypeError),
        (device.create_control("a", ControlType.VALUE, order="x"), TypeError),
        (device.create_control("a", ControlType.VALUE, order=2.5), TypeError),
        (device.create_control("a", ControlType.VALUE, order=float("nan")), TypeError),
        (device.create_control("a", ControlType.VALUE, readonly="yes"), TypeError),
        (device.create_control("a", ControlType.VALUE, hidden=None), TypeError),
        (device.create_control("a/b", ControlType.VALUE), ValueError),
        (device.create_control("x" * 70000, ControlType.VALUE, value=1), ValueError),
    ]
    for call, error in calls:
        with pytest.raises(error):
            await call

    assert not [
        topic for topic in await read_retained(broker.port, "/devices/dev/#") if "/controls/" in topic
    ]
    control = await device.create_control("a", ControlType.VALUE, value=1)
    assert control.exists and control.meta.order == 1


async def test_error_flags_go_out_as_letters_and_come_back(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    Combined flags are written as letters, letters other than r, w and p are ignored on reading.
    """
    temp = await (await driver.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    remote = await client.get_control("dev/temp")

    await temp.set_error(ControlErrorFlags.READ | ControlErrorFlags.POLL)

    error_topic = "/devices/dev/controls/temp/meta/error"
    await wait_retained(broker.port, error_topic, {error_topic: b"rp"})
    await wait_until(lambda: remote.error == ControlErrorFlags.READ | ControlErrorFlags.POLL)
    for letters, flags in [
        ("rwp", ControlErrorFlags.READ | ControlErrorFlags.WRITE | ControlErrorFlags.POLL),
        ("x", ControlErrorFlags.NONE),
        ("w", ControlErrorFlags.WRITE),
    ]:
        await driver.raw.publish("/devices/dev/controls/temp/meta/error", letters, retain=True)
        await wait_until(lambda expected=flags: remote.error == expected)


async def test_error_of_another_type_is_refused_before_anything_changes(
    broker: MosquittoBroker, client: Client
) -> None:
    """
    set_error of a control or of the device takes only ControlErrorFlags. Anything else raises TypeError
    before the state changes, nothing is published.
    """
    device = await client.create_device("dev")
    temp = await device.create_control("temp", ControlType.VALUE, value=1)

    for error in ("r", None, 1):
        with pytest.raises(TypeError):
            await temp.set_error(error)
        with pytest.raises(TypeError):
            await device.set_error(error)

    assert temp.error == ControlErrorFlags.NONE and device.error == ControlErrorFlags.NONE
    retained = await read_retained(broker.port, "/devices/dev/#")
    assert not [topic for topic in retained if topic.endswith("/error")]
    await temp.set_error(ControlErrorFlags.READ)
    error_topic = "/devices/dev/controls/temp/meta/error"
    await wait_retained(broker.port, error_topic, {error_topic: b"r"})


async def test_enum_of_the_meta_is_read_only(broker: MosquittoBroker, client: Client) -> None:
    """
    The enum in control.meta cannot be changed in place, a changed copy goes out through update_meta.
    """
    mode = await (await client.create_device("dev")).create_control(
        "mode", ControlType.VALUE, enum={"0": "Off"}, value=0
    )

    with pytest.raises(TypeError):
        mode.meta.enum["1"] = "On"  # type: ignore[index]
    enum = dict(mode.meta.enum)
    enum["1"] = "On"
    await mode.update_meta(enum=enum)

    meta = {
        "type": "value",
        "readonly": False,
        "hidden": False,
        "order": 1,
        "enum": {"0": {"en": "Off"}, "1": {"en": "On"}},
    }
    meta_topic = "/devices/dev/controls/mode/meta"
    await wait_retained(broker.port, meta_topic, {meta_topic: json.dumps(meta).encode()})


@pytest.mark.parametrize("control_type", [ControlType.VALUE, ControlType.SWITCH, ControlType.TEXT])
async def test_none_clears_the_own_value(
    broker: MosquittoBroker, client: Client, control_type: ControlType
) -> None:
    """
    set_value(None) erases the retained value, the value becomes None without an event, and the value
    does not come back after reconnect. A value set again goes out as usual.
    """
    initial = {ControlType.VALUE: 5, ControlType.SWITCH: True, ControlType.TEXT: "on"}[control_type]
    control = await (await client.create_device("dev")).create_control("c", control_type, value=initial)
    events: list = []
    control.on_value(lambda _control, value, previous: events.append(value))
    await wait_until(lambda: events == [initial])

    await control.set_value(None)

    assert control.value is None
    await wait_retained(broker.port, "/devices/dev/controls/c", {})
    await restart_broker(broker, client)
    meta = f'{{"type": "{control_type}", "readonly": false, "hidden": false, "order": 1}}'.encode()
    await wait_retained(broker.port, "/devices/dev/controls/c/#", {"/devices/dev/controls/c/meta": meta})
    await control.set_value(initial)
    await wait_until(lambda: events == [initial, initial])


async def test_empty_text_erases_the_retained_value_and_stays_empty_locally(
    broker: MosquittoBroker, connect: ConnectClient, client: Client
) -> None:
    """
    An empty string in text erases the retained value, MQTT keeps no empty retained message. The own value
    becomes "", a reader has None. A text set afterwards goes out as usual.
    """
    status = await (await client.create_device("dev")).create_control("status", ControlType.TEXT, value="on")
    reader = await connect("reader")
    remote = await reader.get_control("dev/status")
    assert remote.value == "on"

    await status.set_value("")

    assert status.value == ""
    await wait_retained(broker.port, "/devices/dev/controls/status", {})
    await wait_until(lambda: remote.value is None)
    await status.set_value("off")
    await wait_until(lambda: remote.value == "off")


@pytest.mark.parametrize(
    "title, lang, expected",
    [
        (TranslatedTitle("Temperature", "Температура"), "ru", "Температура"),
        (TranslatedTitle("Temperature", "Температура"), "en", "Temperature"),
        (TranslatedTitle("Temperature"), "ru", "Temperature"),
        (TranslatedTitle(ru="Температура"), "en", "Температура"),
        (TranslatedTitle(), "ru", "temp"),
    ],
    ids=["ru", "en", "ru falls back to en", "en falls back to ru", "default"],
)
def test_title_in_a_language_falls_back_to_the_other_one_then_to_the_default(
    title: TranslatedTitle, lang: str, expected: str
) -> None:
    """
    title.get gives the text in the language, else in the other one, else the default, as homeui shows it.
    """
    assert title.get(lang, "temp") == expected


async def test_value_with_precision_changes_locally_also_when_its_payload_stays(
    connect: ConnectClient, client: Client
) -> None:
    """
    Precision rounds only the MQTT payload. A new number with the same payload changes control.value
    and reaches on_value, the payload does not go out again.
    """
    device = await client.create_device("precision_dev")
    level = await device.create_control("level", ControlType.VALUE, precision=0.1, value=21.54)
    events: list[tuple[float, Optional[float]]] = []
    level.on_value(lambda _control, value, previous: events.append((value, previous)))
    await wait_until(lambda: events == [(21.54, None)])
    assert level.value == 21.54

    watcher = await connect("precision-watcher")
    value_topic = "/devices/precision_dev/controls/level"
    barrier_topic = "precision-test/barrier"
    # The first payload is the state at subscription, the rest are publications
    payloads: list[bytes] = []
    barriers: list[bytes] = []

    async def observe(message: Message) -> None:
        if message.topic == value_topic:
            payloads.append(message.payload)
        else:
            barriers.append(message.payload)

    await watcher.raw.subscribe(value_topic, observe)
    await watcher.raw.subscribe(barrier_topic, observe)
    await wait_until(lambda: payloads == [b"21.5"])

    async def settle(token: bytes) -> None:
        await client.raw.publish(barrier_topic, token)
        await wait_until(lambda: barriers[-1:] == [token])

    await level.set_value(21.53)
    await wait_until(lambda: events[-1:] == [(21.53, 21.54)])
    await settle(b"same-payload")
    assert level.value == 21.53
    assert payloads == [b"21.5"]

    await level.set_value(21.53)
    await settle(b"same-value")
    assert events == [(21.54, None), (21.53, 21.54)]
    assert payloads == [b"21.5"]

    await level.set_value(21.43)
    await wait_until(lambda: events[-1:] == [(21.43, 21.53)])
    await settle(b"changed-payload")
    assert level.value == 21.43
    assert payloads == [b"21.5", b"21.4"]
