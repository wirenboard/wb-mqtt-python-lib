"""
Foreign devices: live objects built from broker messages, appearance, commands, one-off reads.
"""

import asyncio
import json
import math
from typing import Any

import pytest

from tests.broker import (
    DeadLinkProxy,
    MosquittoBroker,
    device_object,
    internals,
    publish_retained,
    read_retained,
    wait_or_dump,
    wait_retained,
    wait_until,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import (
    Client,
    Control,
    ControlErrorFlags,
    ControlMissingError,
    ControlType,
    Device,
    NotOwnError,
    TranslatedTitle,
)


@pytest.mark.parametrize(
    "control_type, payload, expected",
    [
        ("switch", "1", True),
        ("switch", "0", False),
        ("value", "21.5", 21.5),
        ("value", "7", 7),
        ("range", "100", 100),
        ("rgb", "255;0;10", (255, 0, 10)),
        ("text", "hello", "hello"),
        ("voltage", "24.1", 24.1),
        ("future_type", "abc", "abc"),
    ],
)
async def test_value_comes_typed_by_meta(
    driver: Client, client: Client, control_type: str, payload: str, expected: object
) -> None:
    """
    The service gets switch as bool, value and range as numbers, rgb as three numbers.

    A deprecated type derived from value comes as a number too, an unknown type comes as the string
    from the topic, without a guess at its type.
    """
    base = "/devices/dev/controls/ctl"
    await publish_retained(driver, {f"{base}/meta": json.dumps({"type": control_type}), base: payload})

    control = await client.get_control("dev/ctl")

    assert control.exists and control.value == expected


async def test_foreign_precision_does_not_round_the_received_payload(driver: Client, client: Client) -> None:
    """
    A foreign value comes as the broker sent it, the precision of the driver does not round it again.
    """
    await publish_retained(
        driver,
        {
            "/devices/dev/meta": '{"driver":"driver"}',
            "/devices/dev/controls/ctl/meta": '{"type":"value","precision":0.1}',
            "/devices/dev/controls/ctl": "21.54",
        },
    )

    control = await client.get_control("dev/ctl")

    assert control.value == 21.54


async def test_command_to_foreign_control_goes_to_on_and_value_follows_driver(
    driver: Client, client: Client
) -> None:
    """
    set_value of a foreign control is a command, readonly too, the value changes when the driver publishes it.
    """
    relay = await (await driver.create_device("relays")).create_control(
        "K1", ControlType.SWITCH, value=False, readonly=True
    )
    received, release = asyncio.Event(), asyncio.Event()

    async def apply(control: Control, value: Any) -> None:
        received.set()
        await release.wait()
        await control.set_value(value)

    relay.on_command(apply)
    remote = await client.get_control("relays/K1")

    with pytest.raises(TypeError):
        await remote.set_value(1)
    assert relay.value is False
    await remote.set_value(True)
    await wait_or_dump(received.wait(), timeout=5)
    assert remote.value is False
    release.set()

    assert await wait_or_dump(remote.wait_value(True, timeout=5), timeout=6) is True


async def test_error_comes_from_meta_error_topic_and_json_meta(driver: Client, client: Client) -> None:
    """
    homeui reads both, the last message wins.
    """
    base = "/devices/dev/controls/ctl"
    await publish_retained(driver, {f"{base}/meta": json.dumps({"type": "value"}), base: "1"})
    control = await client.get_control("dev/ctl")
    errors = []
    control.on_error(lambda c, error, previous: errors.append(error))

    await driver.raw.publish(f"{base}/meta/error", "r", retain=True)
    await wait_until(lambda: errors[-1:] == [ControlErrorFlags.READ])
    await driver.raw.publish(f"{base}/meta", json.dumps({"type": "value", "error": "w"}), retain=True)
    await wait_until(lambda: errors[-1:] == [ControlErrorFlags.WRITE])

    assert control.error == ControlErrorFlags.WRITE


async def test_foreign_control_rejects_operations_of_the_owner(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    The library never changes meta, errors or existence of a foreign device, of its controls or of itself.
    """
    await (await driver.create_device("dev")).create_control("ctl", ControlType.VALUE, value=1)
    control = await client.get_control("dev/ctl")
    device = await client.get_device("dev")
    before = await read_retained(broker.port, "/devices/dev/#")

    with pytest.raises(NotOwnError):
        await control.set_error(ControlErrorFlags.READ)
    with pytest.raises(NotOwnError):
        await control.update_meta(title="x")
    with pytest.raises(NotOwnError):
        await control.remove()
    with pytest.raises(NotOwnError):
        control.on_command(lambda c, value: None)
    with pytest.raises(NotOwnError):
        await device.create_control("new", ControlType.VALUE)
    with pytest.raises(NotOwnError):
        await device.set_error(ControlErrorFlags.READ)
    with pytest.raises(NotOwnError):
        await device.update_meta(title="x")
    with pytest.raises(NotOwnError):
        await device.remove()

    await asyncio.sleep(0.2)
    assert await read_retained(broker.port, "/devices/dev/#") == before


async def test_get_device_names_reads_once_without_keeping_subscriptions(
    driver: Client, client: Client
) -> None:
    """
    Devices with JSON meta and devices known only by their controls, read once. Broken device meta
    and control meta without a type make no device, as for a live object and a snapshot.
    """
    await driver.create_device("with_meta")
    await (await driver.create_device("with_controls")).create_control("ctl", ControlType.VALUE, value=1)
    await publish_retained(
        driver,
        {
            "/devices/legacy/meta/name": "Legacy",
            "/devices/legacy/controls/state/meta": json.dumps({"type": "text"}),
            "/devices/broken/meta": "[",
            "/devices/untyped/controls/state/meta": json.dumps({"title": "State"}),
        },
    )

    names = await client.get_device_names()

    assert names == ["legacy", "with_controls", "with_meta"]
    assert internals(client)["subscriptions"] == 0


async def test_device_snapshot_reads_once_and_stays_as_read(driver: Client, client: Client) -> None:
    """
    The snapshot carries the title, the driver, typed values and errors, and leaves no subscription.

    It does not follow later changes. A name without a device gives None.
    """
    device = await driver.create_device("dev", title={"en": "Device", "ru": "Устройство"})
    temp = await device.create_control("temp", ControlType.VALUE, units="deg C", value=21.5)
    relay = await device.create_control("relay", ControlType.SWITCH, value=True)
    await relay.set_error(ControlErrorFlags.READ)
    await device.create_control("button", ControlType.PUSHBUTTON)
    # A retained value on a pushbutton is a leftover, not a value
    await driver.raw.publish("/devices/dev/controls/button", "1", retain=True)
    await driver.raw.publish("/devices/dev/controls/no_meta", "5", retain=True)

    snapshot = await client.get_device_snapshot("dev")
    await temp.set_value(30)

    assert (snapshot.name, snapshot.driver) == ("dev", "driver")
    assert snapshot.title == TranslatedTitle(en="Device", ru="Устройство")
    assert set(snapshot.controls) == {"temp", "relay", "button"}
    assert snapshot.controls["temp"].value == 21.5 and snapshot.controls["temp"].meta.units == "deg C"
    assert snapshot.controls["relay"].value is True
    assert snapshot.controls["relay"].error == ControlErrorFlags.READ
    assert snapshot.controls["button"].value is None
    assert internals(client)["subscriptions"] == 0
    assert await client.get_device_snapshot("missing") is None


async def test_device_back_without_its_meta_has_no_old_title_or_driver(
    connect: ConnectClient, driver: Client, client: Client
) -> None:
    """
    A device is erased and appears again with controls only. The object shows no title and no driver,
    like a snapshot of it, and not those of the erased meta.
    """
    device = await driver.create_device("X", title="Old")
    await device.create_control("a", ControlType.VALUE, value=1)
    remote = await client.get_device("X")
    await device.remove()
    await wait_until(lambda: not remote.exists)
    other = await connect("other")

    await other.raw.publish("/devices/X/controls/b/meta", '{"type": "value"}', retain=True)

    await wait_until(lambda: remote.exists)
    snapshot = await client.get_device_snapshot("X")
    assert (remote.driver, remote.title) == (snapshot.driver, snapshot.title) == (None, TranslatedTitle())


async def test_device_meta_erased_while_its_controls_stay_is_a_meta_change(
    driver: Client, client: Client
) -> None:
    """
    The JSON meta of a device is erased and its controls stay. The device still exists, on_meta comes
    with no title and no driver.
    """
    device = await driver.create_device("X", title="Old")
    await device.create_control("a", ControlType.VALUE, value=1)
    remote = await client.get_device("X")
    metas: list[tuple[TranslatedTitle, Any]] = []
    remote.on_meta(lambda changed: metas.append((changed.title, changed.driver)))

    await driver.raw.publish("/devices/X/meta", "", retain=True)

    await wait_until(lambda: metas == [(TranslatedTitle(), None)])
    assert remote.exists


async def test_foreign_pushbutton_presses_and_a_retained_leftover(
    driver: Client, dead_link: tuple[DeadLinkProxy, Client]
) -> None:
    """
    A live press is an event with True. A retained message on a pushbutton is a leftover, not a press, also
    when the broker sends it again after a reconnect while a handler listens, and when it is published
    while the handler listens.
    """
    proxy, client = dead_link
    await publish_retained(
        driver,
        {"/devices/dev/controls/button/meta": '{"type": "pushbutton"}', "/devices/dev/controls/button": "1"},
    )
    button = await client.get_control("dev/button")
    presses: list = []
    button.on_value(lambda control, value, previous: presses.append(value))
    await proxy.drop(client)
    await proxy.restore(client)
    # The end of a new read comes after the retained messages of the subscriptions made again
    await client.raw.read("/devices/dev/controls/button")
    await asyncio.sleep(0.2)
    assert not presses and button.value is None and button.exists

    await driver.raw.publish("/devices/dev/controls/button", "1", retain=True)
    await client.raw.read("/devices/dev/controls/button")
    await asyncio.sleep(0.2)
    assert not presses

    for _ in range(2):
        await driver.raw.publish("/devices/dev/controls/button", "1")

    await wait_until(lambda: presses == [True, True])


async def test_operations_on_a_control_that_is_gone_raise_control_missing(
    connect: ConnectClient, driver: Client, client: Client
) -> None:
    """
    A foreign control during a driver restart, an own control after remove(), a control of a removed own
    device and the removed device itself.

    remove() and on_command of a control of a removed own device do not raise. The handler waits until
    the service creates the control again.
    """
    device = await driver.create_device("dev")
    temp = await device.create_control("temp", ControlType.VALUE, value=1)
    level = await device.create_control("level", ControlType.VALUE, value=1)
    remote = await client.get_control("dev/temp")

    await temp.remove()
    await wait_until(lambda: not remote.exists)

    with pytest.raises(ControlMissingError):
        await remote.set_value(2)
    for call in (temp.set_value(2), temp.set_error(ControlErrorFlags.READ), temp.update_meta(units="V")):
        with pytest.raises(ControlMissingError):
            await call
    await device.remove()
    for call in (
        level.set_value(2),
        level.set_error(ControlErrorFlags.READ),
        level.update_meta(units="V"),
        device.set_error(ControlErrorFlags.READ),
        device.update_meta(title="New"),
        device.create_control("new", ControlType.VALUE),
    ):
        with pytest.raises(ControlMissingError):
            await call
    await level.remove()
    commands = []
    level.on_command(lambda control, value: commands.append(value))

    await (await driver.create_device("dev")).create_control("level", ControlType.VALUE, value=1)
    user = await connect("user")
    await user.raw.publish("/devices/dev/controls/level/on", "5")

    await wait_until(lambda: commands == [5])


@pytest.mark.parametrize("appearance", ["device and control", "control meta only", "never"])
async def test_wait_ends_when_the_name_appears_or_at_the_timeout(
    driver: Client, client: Client, appearance: str
) -> None:
    """
    get_control and get_device with a timeout return as soon as the name appears, not at the timeout.
    A name that does not appear in time gives TimeoutError.
    """
    timeout = 1 if appearance == "never" else 5
    control_wait = asyncio.ensure_future(client.get_control("late/temp", timeout=timeout))
    device_wait = asyncio.ensure_future(client.get_device("late", timeout=timeout))
    await wait_until(lambda: internals(client)["waits"] == 2)

    if appearance == "device and control":
        await (await driver.create_device("late")).create_control("temp", ControlType.VALUE, value=1)
    elif appearance == "control meta only":
        await driver.raw.publish("/devices/late/controls/temp/meta", '{"type": "value"}', retain=True)

    results = await wait_or_dump(asyncio.gather(control_wait, device_wait, return_exceptions=True), timeout=3)
    if appearance == "never":
        assert [type(result) for result in results] == [TimeoutError, TimeoutError]
    else:
        assert all(result.exists for result in results)
    assert internals(client)["waits"] == 0


async def test_malformed_foreign_meta_does_not_break_reading(connect: ConnectClient) -> None:
    """
    Any client may publish anything into /devices. A broken field is dropped, a broken meta is ignored,
    and the cleanup of the own driver still works.
    """
    other = await connect("other")
    await publish_retained(
        other,
        {
            "/devices/bad/meta": "[" * 100000,
            "/devices/bad/controls/notype/meta": '{"title": "no type"}',
            "/devices/bad/controls/odd/meta": (
                '{"type": "value", "title": 1, "enum": [1], "units": 5, "order": true, '
                '"precision": Infinity}'
            ),
            "/devices/bad/controls/odd": "21.5",
            "/devices/weird/meta": '{"title": {"en": 5}, "driver": {"name": "x"}}',
        },
    )
    client = await connect("service")

    odd = await client.get_control("bad/odd", timeout=5)
    bad = await client.get_device("bad")
    weird = await client.get_device("weird", timeout=5)
    await client.remove_driver_devices()

    assert (odd.value, odd.title, odd.meta.enum, odd.units, odd.meta.order, odd.meta.precision) == (
        21.5,
        TranslatedTitle(),
        None,
        None,
        None,
        None,
    )
    await odd.set_value(1.25)
    assert list(bad.controls) == ["odd"] and bad.driver is None
    assert weird.exists and weird.driver is None and weird.title == TranslatedTitle()


async def test_foreign_meta_numbers_out_of_range_are_dropped(connect: ConnectClient) -> None:
    """
    A precision too large for a float, NaN and infinite limits are dropped from a foreign meta. The control
    reads, a snapshot works, the same meta delivered again is no change, and create_device with force
    takes the name.
    """
    other = await connect("other")
    meta = '{"type": "value", "precision": 1%s, "min": NaN, "max": -Infinity}' % ("0" * 400)
    await publish_retained(
        other,
        {
            "/devices/odd/meta": '{"driver": "other"}',
            "/devices/odd/controls/level/meta": meta,
            "/devices/odd/controls/level": "5",
        },
    )
    client = await connect("service")
    level = await client.get_control("odd/level", timeout=5)
    metas: list[str] = []
    level.on_meta(lambda control: metas.append(control.name))

    await other.raw.publish("/devices/odd/controls/level/meta", meta, retain=True)
    await other.raw.publish("/devices/odd/controls/level", "6", retain=True)
    await wait_until(lambda: level.value == 6)

    assert (level.meta.precision, level.meta.minimum, level.meta.maximum) == (None, None, None)
    assert not metas
    snapshot = await client.get_device_snapshot("odd")
    assert snapshot.controls["level"].meta.precision is None
    device = await client.create_device("odd", force=True)
    assert device.is_own


async def test_nan_after_nan_is_no_change(driver: Client, client: Client) -> None:
    """
    wb-rules publishes NaN as «NaN». NaN is not equal to itself, still the same NaN again is no new value.
    """
    await (await driver.create_device("dev")).create_control("level", ControlType.VALUE, value=1)
    level = await client.get_control("dev/level")
    values: list[float] = []
    level.on_value(lambda control, value, previous: values.append(value))

    for payload in ("NaN", "NaN", "2"):
        await driver.raw.publish("/devices/dev/controls/level", payload, retain=True)

    await wait_until(lambda: len(values) >= 3 and values[-1] == 2)
    assert len(values) == 3 and values[0] == 1 and math.isnan(values[1])


async def test_control_name_without_a_slash_is_refused(client: Client) -> None:
    """
    get_control takes «device/control». A name without the slash is refused before subscribing.
    """
    with pytest.raises(ValueError):
        await client.get_control("dev")

    assert internals(client)["subscriptions"] == 0


async def test_cancelled_get_control_does_not_cancel_the_shared_subscription(
    driver: Client, client: Client
) -> None:
    """
    Two callers wait for one subscription to a new device. Cancelling one leaves the other and later calls.
    """
    await (await driver.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    first = asyncio.ensure_future(client.get_control("dev/temp"))
    second = asyncio.ensure_future(client.get_control("dev/temp"))
    await asyncio.sleep(0)

    first.cancel()

    control = await wait_or_dump(second, timeout=3)
    with pytest.raises(asyncio.CancelledError):
        await first
    subscriptions = internals(client)["subscriptions"]
    again = await wait_or_dump(client.get_control("dev/temp"), timeout=3)
    assert again == control and control.value == 1 and internals(client)["subscriptions"] == subscriptions


async def test_subscription_that_fails_after_its_caller_gave_up_is_tried_again(
    driver: Client, dead_link: tuple[DeadLinkProxy, Client]
) -> None:
    """
    get_device is cancelled while the broker does not answer, then the link drops and the subscription fails.

    The device does not count as read, a handler on it is refused, and the next get_device subscribes again.
    """
    await (await driver.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    proxy, client = dead_link
    proxy.dead = True
    attempt = asyncio.ensure_future(client.get_device("dev"))
    await asyncio.sleep(0.3)
    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt

    proxy.dead = False
    proxy.cut()

    def on_added(_device: Device) -> None:
        pass

    def handler_refused() -> bool:
        # The subscription fails once its wait for the broker ends, up to the 5 s of an answer
        try:
            device_object(client, "dev").on_added(on_added)
        except RuntimeError:
            return True
        device_object(client, "dev").remove_handler(on_added)
        return False

    await wait_until(handler_refused, timeout=15)
    await wait_until(lambda: client.connected, timeout=10)
    device = await wait_or_dump(client.get_device("dev"), timeout=5)
    assert device.exists and device.controls["temp"].value == 1


async def test_foreign_device_error_is_read_live_and_in_a_snapshot(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    The error of a foreign device follows the broker, the snapshot keeps the error it read.
    """
    device = await driver.create_device("dev")
    await device.create_control("temp", ControlType.VALUE, value=1)
    await device.set_error(ControlErrorFlags.READ)
    await wait_retained(broker.port, "/devices/dev/meta/error", {"/devices/dev/meta/error": b"r"})
    remote = await client.get_device("dev")
    errors = []
    remote.on_error(lambda _device, error, previous: errors.append((error, previous)))
    snapshot = await client.get_device_snapshot("dev")

    await device.set_error(ControlErrorFlags.NONE)

    await wait_until(lambda: len(errors) == 2)
    assert errors == [(ControlErrorFlags.READ, None), (ControlErrorFlags.NONE, ControlErrorFlags.READ)]
    assert remote.error == ControlErrorFlags.NONE and snapshot.error == ControlErrorFlags.READ


@pytest.mark.parametrize(
    "case",
    [
        ("value", "abc", "7", 7),
        ("switch", "off", "1", True),
        ("local_time", "1.5", "1700000000", 1700000000),
        ("rgb", "1;2", "1;2;3", (1, 2, 3)),
    ],
    ids=["value", "switch", "local_time", "rgb"],
)
async def test_payload_that_does_not_parse_comes_as_a_string(
    driver: Client, client: Client, case: tuple[str, str, str, object]
) -> None:
    """
    A value or a command that does not parse by the type reaches the service as the string from the topic,
    the next good one comes typed again.
    """
    control_type, garbage, good, expected = case
    base = "/devices/dev/controls/ctl"
    commands: list = []
    own = await (await driver.create_device("dev")).create_control("ctl", ControlType(control_type))
    own.on_command(lambda _control, value: commands.append(value))
    remote = await client.get_control("dev/ctl")
    values: list = []
    remote.on_value(lambda _control, value, _previous: values.append(value))

    for payload in (garbage, good):
        await driver.raw.publish(base, payload, retain=True)
        await client.raw.publish(f"{base}/on", payload)

    await wait_until(lambda: values == [garbage, expected] and commands == [garbage, expected])


@pytest.mark.parametrize(
    "case",
    [
        ("value", "switch", "1", True),
        ("switch", "value", "1", 1),
        ("switch", "value", "0", 0),
        ("value", "pushbutton", "1", None),
        ("text", "value", "1", 1),
    ],
    ids=["value to switch", "switch to value", "switch off to value", "value to pushbutton", "text to value"],
)
async def test_new_type_of_a_foreign_control_reads_its_value_again_without_an_event(
    driver: Client, client: Client, case: tuple[str, str, str, object]
) -> None:
    """
    A driver publishes the meta of a control again with another type. It is a meta change, the value is
    read again by the new type without on_value, a pushbutton has no value and no press. A wait for the value
    of the new type, by value and type, returns.
    """
    before, after, payload, expected = case
    base = "/devices/dev/controls/ctl"
    await publish_retained(driver, {f"{base}/meta": json.dumps({"type": before}), base: payload})
    remote = await client.get_control("dev/ctl")
    values: list = []
    metas: list = []
    remote.on_value(lambda _control, value, _previous: values.append(value))
    remote.on_meta(lambda control: metas.append(control.control_type))
    await wait_until(lambda: len(values) == 1)

    def fits(value: Any) -> bool:
        # By type too, 1 == True, and the value of the old type would fit at once
        return value == expected and type(value) is type(expected)

    waiting = None if expected is None else asyncio.ensure_future(remote.wait_value(fits, timeout=5))

    await driver.raw.publish(f"{base}/meta", json.dumps({"type": after}), retain=True)

    await wait_until(lambda: metas == [after])
    assert remote.value == expected and type(remote.value) is type(expected)
    if waiting is not None:
        assert await wait_or_dump(waiting, timeout=6) == expected
    await asyncio.sleep(0.2)
    assert len(values) == 1
