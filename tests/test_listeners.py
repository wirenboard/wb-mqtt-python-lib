"""
Listeners and waits for a value: the call at registration, idempotent registration, order and independence
of queues.
"""

import asyncio
from typing import Any

import pytest

from tests.broker import (
    MosquittoBroker,
    control_object,
    device_object,
    internals,
    publish_retained,
    wait_or_dump,
    wait_retained,
    wait_until,
)
from wb_mqtt_lib import Client, Control, ControlErrorFlags, ControlType, Device


async def test_state_listeners_fire_at_registration_with_previous_none(
    driver: Client, client: Client
) -> None:
    """
    on_added and on_value describe the current state at once, on_meta waits for a change.
    """
    await (await driver.create_device("dev")).create_control("temp", ControlType.VALUE, value=3)
    control = await client.get_control("dev/temp")
    calls = []

    control.on_added(lambda c: calls.append("added"))
    control.on_value(lambda c, value, previous: calls.append(("value", value, previous)))
    control.on_meta(lambda c: calls.append("meta"))

    await wait_until(lambda: len(calls) == 2)
    assert sorted(calls, key=str) == sorted(["added", ("value", 3, None)], key=str)


async def test_value_and_error_that_came_before_the_meta_reach_handlers_at_appearance(
    driver: Client, client: Client
) -> None:
    """
    A value of a control and an error of a control or of the device that came before the meta reach on_value
    and on_error registered earlier when the meta arrives.
    """
    device = await client.get_device("dev")
    control = await client.get_control("dev/temp")
    calls: list = []
    control.on_value(lambda c, value, previous: calls.append(("value", value, previous)))
    control.on_error(lambda c, error, previous: calls.append(("control", error, previous)))
    device.on_error(lambda d, error, previous: calls.append(("device", error, previous)))

    await publish_retained(
        driver,
        {
            "/devices/dev/controls/temp/meta/error": "r",
            "/devices/dev/meta/error": "w",
            "/devices/dev/meta": '{"driver": "driver"}',
            "/devices/dev/controls/temp": "21",
            "/devices/dev/controls/temp/meta": '{"type": "value"}',
        },
    )

    await wait_until(lambda: len(calls) == 3)
    assert sorted(calls, key=str) == [
        ("control", ControlErrorFlags.READ, None),
        ("device", ControlErrorFlags.WRITE, None),
        ("value", 21, None),
    ]


async def test_same_handler_registered_twice_is_called_once(level: Control) -> None:
    """
    The second registration of the same handler adds nothing.
    """
    calls = []

    def handler(_control: Control, value: Any, _previous: Any) -> None:
        calls.append(value)

    level.on_value(handler)
    level.on_value(handler)
    await wait_until(lambda: calls == [0])
    await level.set_value(1)

    await wait_until(lambda: calls[-1:] == [1])
    assert calls == [0, 1]


async def test_on_methods_give_the_handler_back(client: Client, level: Control) -> None:
    """
    on_* of a control, a device and the client give the handler back, so they work as decorators.
    """

    def on_value(_control: Control, _value: Any, _previous: Any) -> None:
        pass

    def on_added(_device: Device) -> None:
        pass

    def on_connection_change(_connected: bool) -> None:
        pass

    assert level.on_value(on_value) is on_value
    assert level.device.on_added(on_added) is on_added
    assert client.on_connection_change(on_connection_change) is on_connection_change


async def test_listener_gets_every_value_of_a_burst_in_order(driver: Client, client: Client) -> None:
    """
    A handler slower than the stream still gets every value with its own previous, in order.
    """
    level = await (await driver.create_device("dev")).create_control("level", ControlType.VALUE, value=0)
    remote = await client.get_control("dev/level")
    seen = []

    async def handler(_control: Control, value: Any, previous: Any) -> None:
        await asyncio.sleep(0)  # a handler that awaits must still see events in order
        seen.append((value, previous))

    remote.on_value(handler)
    for value in range(1, 201):
        await level.set_value(value)

    await wait_until(lambda: len(seen) == 201, timeout=10)
    assert seen == [(0, None)] + [(value, value - 1) for value in range(1, 201)]


async def test_slow_listener_does_not_delay_another(level: Control) -> None:
    """
    A handler stuck for half a minute does not hold the queue of another handler.
    """
    slow_started = asyncio.Event()
    fast_seen = []

    async def slow(_control: Control, _value: Any, _previous: Any) -> None:
        slow_started.set()
        await asyncio.sleep(30)

    level.on_value(slow)
    level.on_value(lambda control, value, previous: fast_seen.append(value))
    await wait_or_dump(slow_started.wait(), timeout=2)
    await level.set_value(1)

    await wait_until(lambda: fast_seen == [0, 1], timeout=1)


async def test_device_listeners_cover_existing_and_new_controls(driver: Client, client: Client) -> None:
    """
    Device listeners get existing controls at once and controls created later.
    """
    device = await driver.create_device("dev")
    first = await device.create_control("first", ControlType.VALUE, value=1)
    remote = await client.get_device("dev")
    added, values = [], []
    remote.on_control_added(lambda control: added.append(control.name))
    remote.on_control_value(lambda control, value, previous: values.append((control.name, value)))

    await device.create_control("second", ControlType.VALUE, value=2)
    await first.set_value(10)

    await wait_until(lambda: dict(values) == {"first": 10, "second": 2})
    assert added == ["first", "second"]


async def test_device_control_error_fires_at_registration_and_control_meta_on_change(
    broker: MosquittoBroker, driver: Client, client: Client
) -> None:
    """
    on_control_error describes every existing control at once, on_control_meta waits for a change.
    """
    device = await driver.create_device("dev")
    first = await device.create_control("first", ControlType.VALUE, value=1)
    second = await device.create_control("second", ControlType.VALUE, value=2)
    await second.set_error(ControlErrorFlags.READ)
    error_topic = "/devices/dev/controls/second/meta/error"
    await wait_retained(broker.port, error_topic, {error_topic: b"r"})
    remote = await client.get_device("dev")
    errors, metas = [], []
    remote.on_control_error(lambda control, error, previous: errors.append((control.name, error, previous)))
    remote.on_control_meta(lambda control: metas.append((control.name, control.meta.units)))
    await wait_until(lambda: len(errors) == 2)

    await first.set_error(ControlErrorFlags.WRITE)
    await second.update_meta(units="V")

    await wait_until(lambda: len(errors) == 3 and len(metas) == 1)
    assert errors == [
        ("first", ControlErrorFlags.NONE, None),
        ("second", ControlErrorFlags.READ, None),
        ("first", ControlErrorFlags.WRITE, ControlErrorFlags.NONE),
    ]
    assert metas == [("second", "V")]


async def test_every_listener_waits_for_a_control_that_does_not_exist_yet(
    driver: Client, client: Client
) -> None:
    """
    The service starts before the driver. Listeners and a wait hang on a control before it appears,
    of a foreign device and of an own one.
    """
    foreign = await client.get_control("dev/temp")
    events: dict[str, list] = {"added": [], "value": [], "error": [], "meta": [], "removed": []}
    foreign.on_added(events["added"].append)
    foreign.on_value(lambda control, value, previous: events["value"].append(value))
    foreign.on_error(lambda control, error, previous: events["error"].append(error))
    foreign.on_meta(lambda control: events["meta"].append(control.meta.units))
    foreign.on_removed(events["removed"].append)
    value_wait = asyncio.ensure_future(foreign.wait_value(1, timeout=5))
    own_device = await client.create_device("own")
    later = await client.get_control("own/later")
    commands = []
    later.on_command(lambda control, value: commands.append(value))
    assert not foreign.exists and foreign.value is None and not later.exists

    device = await driver.create_device("dev")
    temp = await device.create_control("temp", ControlType.VALUE, value=1)
    assert await wait_or_dump(value_wait, timeout=6) == 1
    await temp.set_error(ControlErrorFlags.READ)
    await temp.update_meta(units="V")
    await temp.remove()
    await own_device.create_control("later", ControlType.SWITCH, value=False)
    await driver.raw.publish("/devices/own/controls/later/on", "1")

    await wait_until(lambda: events["removed"] and commands == [True])
    assert events == {
        "added": [foreign],
        "value": [1],
        # the driver erases /meta/error before the meta, so the error is cleared before the removal
        "error": [ControlErrorFlags.READ, ControlErrorFlags.NONE],
        "meta": ["V"],
        "removed": [foreign],
    }


async def test_handler_on_a_device_the_client_does_not_read_is_refused(
    driver: Client, client: Client
) -> None:
    """
    Events of a foreign device reach the client only after get_device or get_control. A handler or a wait
    before that would never be called, so it raises. Own devices need no read.
    """
    temp = await (await driver.create_device("dev")).create_control("temp", ControlType.VALUE, value=1)
    unread = device_object(client, "dev")
    registrations = [
        lambda: unread.on_added(lambda device: None),
        lambda: unread.on_control_value(lambda control, value, previous: None),
        lambda: control_object(client, "dev/temp").on_value(lambda control, value, previous: None),
    ]

    for register in registrations:
        with pytest.raises(RuntimeError):
            register()
    with pytest.raises(RuntimeError):
        await control_object(client, "dev/temp").wait_value(1, timeout=1)
    assert internals(client)["listeners"] == 0

    await client.get_device("dev")
    values = []
    control_object(client, "dev/temp").on_value(lambda control, value, previous: values.append(value))
    await temp.set_value(2)
    await client.create_device("own")
    device_object(client, "own").on_added(lambda device: None)

    await wait_until(lambda: values == [1, 2])


async def test_remove_handler_through_any_object_of_the_name(driver: Client, client: Client) -> None:
    """
    A handler comes off every event of a control or a device, removed through another object of the same name.
    """
    device = await driver.create_device("dev")
    level = await device.create_control("level", ControlType.VALUE, value=1)
    remote = await client.get_control("dev/level")
    remote_device = await client.get_device("dev")
    calls: list = []

    def control_handler(_control: Control, value: Any, _previous: Any) -> None:
        calls.append(("control", value))

    def device_handler(_control: Control, value: Any, _previous: Any) -> None:
        calls.append(("device", value))

    def added(_control: Control) -> None:
        calls.append("added")

    remote.on_value(control_handler)
    remote.on_error(control_handler)
    remote_device.on_control_value(device_handler)
    remote_device.on_control_added(added)
    await wait_until(lambda: len(calls) == 4)
    listeners = internals(client)["listeners"]

    (await client.get_control("dev/level")).remove_handler(control_handler)
    (await client.get_device("dev")).remove_handler(device_handler)
    (await client.get_device("dev")).remove_handler(added)
    await level.set_value(2)
    await device.create_control("other", ControlType.VALUE, value=3)
    await asyncio.sleep(0.3)

    assert internals(client)["listeners"] == listeners - 4 and len(calls) == 4


@pytest.mark.parametrize("swallowed", [False, True], ids=["handler re-raises", "handler swallows"])
async def test_removing_a_busy_handler_cancels_it_and_drops_queued_events(
    client: Client, level: Control, swallowed: bool
) -> None:
    """
    The call that runs now is cancelled at its await, events queued behind it never arrive. So it goes also
    when the handler catches the cancellation and returns, and close() does not wait for such a handler.
    """
    started = asyncio.Event()
    seen: list = []
    cancelled: list = []

    async def slow(_control: Control, value: Any, _previous: Any) -> None:
        seen.append(value)
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(value)
            if not swallowed:
                raise

    level.on_value(slow)
    await wait_or_dump(started.wait(), timeout=2)
    for value in (1, 2, 3):
        await level.set_value(value)

    level.remove_handler(slow)
    await wait_until(lambda: cancelled == [0])
    await asyncio.sleep(0.2)

    assert seen == [0]
    started.clear()
    level.on_value(slow)
    await wait_or_dump(started.wait(), timeout=2)
    await wait_or_dump(client.close(), timeout=5)
    assert cancelled == [0, 3]


async def test_handler_that_removes_itself_finishes_its_call(level: Control) -> None:
    """
    A one-shot handler removes itself and then awaits. Its call is not cancelled and finishes, events
    queued behind it never arrive.
    """
    started = asyncio.Event()
    proceed = asyncio.Event()
    seen: list = []
    finished: list = []

    async def once(control: Control, value: Any, _previous: Any) -> None:
        seen.append(value)
        started.set()
        await proceed.wait()
        control.remove_handler(once)
        await asyncio.sleep(0.05)
        finished.append(value)

    level.on_value(once)
    await wait_or_dump(started.wait(), timeout=2)
    for value in (1, 2, 3):
        await level.set_value(value)
    proceed.set()

    await wait_until(lambda: finished == [0])
    await asyncio.sleep(0.2)
    assert seen == [0] and finished == [0]


async def test_cancelled_error_inside_a_handler_does_not_stop_its_events(level: Control) -> None:
    """
    The handler awaits a task that another part of the service cancelled. The CancelledError it gets
    is a failure of one call, the next events still reach it.
    """
    seen: list = []

    async def handler(_control: Control, value: Any, _previous: Any) -> None:
        seen.append(value)
        if value == 1:
            other = asyncio.ensure_future(asyncio.sleep(5))
            other.cancel()
            await other

    level.on_value(handler)
    for value in (1, 2):
        await level.set_value(value)

    await wait_until(lambda: seen == [0, 1, 2])


async def test_handler_with_the_wrong_arguments_is_refused_at_registration(
    client: Client, level: Control
) -> None:
    """
    A handler that does not take the arguments of its event raises TypeError at registration and is not
    registered.
    """
    device = level.device
    registrations = [
        (level.on_value, lambda control: None),
        (level.on_added, lambda control, value: None),
        (level.on_command, lambda control: None),
        (device.on_control_value, lambda control: None),
        (device.on_added, lambda: None),
        (device.on_error, lambda device: None),
    ]
    for register, handler in registrations:
        with pytest.raises(TypeError):
            register(handler)
    with pytest.raises(TypeError):
        await client.raw.subscribe("/x", lambda: None)

    assert internals(client)["listeners"] == 0
    level.on_value(lambda *args: None)


async def test_device_listener_sees_a_control_removed(driver: Client, client: Client) -> None:
    """
    on_control_removed fires for every control of the device that goes.
    """
    device = await driver.create_device("dev")
    await device.create_control("first", ControlType.VALUE, value=1)
    second = await device.create_control("second", ControlType.VALUE, value=2)
    remote = await client.get_device("dev")
    removed: list = []
    remote.on_control_removed(lambda control: removed.append(control.name))

    await second.remove()

    await wait_until(lambda: removed == ["second"])
    assert list(remote.controls) == ["first"]


async def test_device_listeners_see_a_foreign_device_appear_change_and_go(
    driver: Client, client: Client
) -> None:
    """
    The device appears once for its meta and all its controls, then its title changes and it goes.
    """
    remote = await client.get_device("dev")
    events = []
    remote.on_added(lambda device: events.append("added"))
    remote.on_meta(lambda device: events.append("meta"))
    remote.on_removed(lambda device: events.append("removed"))

    device = await driver.create_device("dev", title="First")
    await device.create_control("a", ControlType.VALUE, value=1)
    await device.create_control("b", ControlType.VALUE, value=2)
    await wait_until(lambda: events == ["added"] and len(remote.controls) == 2)
    await device.update_meta("Second")
    await wait_until(lambda: events == ["added", "meta"] and remote.meta.title.en == "Second")
    await device.remove()

    await wait_until(lambda: events == ["added", "meta", "removed"])
    assert not remote.exists


async def test_device_without_meta_appears_and_goes_with_its_controls(driver: Client, client: Client) -> None:
    """
    wb-nm-helper and wb-cloud-agent publish only /meta/name. Such a device has no driver and exists
    from its first control to its last one.
    """
    await publish_retained(
        driver,
        {"/devices/legacy/meta/name": "Legacy", "/devices/legacy/controls/a/meta": '{"type": "text"}'},
    )
    remote = await client.get_device("legacy")
    events = []
    remote.on_added(lambda device: events.append("added"))
    remote.on_removed(lambda device: events.append("removed"))
    assert remote.exists and remote.meta.driver is None and list(remote.controls) == ["a"]

    await driver.raw.publish("/devices/legacy/controls/b/meta", '{"type": "text"}', retain=True)
    await driver.raw.publish("/devices/legacy/controls/a/meta", "", retain=True)
    await wait_until(lambda: list(remote.controls) == ["b"])
    assert events == ["added"]
    await driver.raw.publish("/devices/legacy/controls/b/meta", "", retain=True)

    await wait_until(lambda: events == ["added", "removed"])


async def test_device_listeners_follow_a_name_from_a_foreign_device_to_an_own_one(
    driver: Client, client: Client
) -> None:
    """
    The foreign device goes and the own one comes once each, when create_device erases it by force.

    Events of the own device come at the call, its echoes from the broker change nothing.
    """
    foreign = await driver.create_device("X", title="Foreign")
    await foreign.create_control("temp", ControlType.VALUE, value=1)
    watched = await client.get_device("X")
    events = []
    watched.on_added(lambda device: events.append(("added", device.meta.driver)))
    watched.on_meta(lambda device: events.append(("meta", device.meta.title.en)))
    watched.on_removed(lambda device: events.append(("removed", None)))
    await wait_until(lambda: len(events) == 1)

    own = await client.create_device("X", title="Mine", force=True)
    await wait_until(lambda: len(events) == 3)
    await own.update_meta("Renamed")
    await wait_until(lambda: len(events) == 4)
    await own.remove()

    await wait_until(lambda: len(events) == 5)
    assert events == [
        ("added", "driver"),
        ("removed", None),
        ("added", "test-service"),
        ("meta", "Renamed"),
        ("removed", None),
    ]


async def test_value_and_error_set_right_before_remove_bring_nothing_back(
    driver: Client, client: Client
) -> None:
    """
    The service reads a name, takes it over and sets values and errors right before remove(). They return
    before PUBACK, so their echoes come from the broker after the removal. No event follows from them.
    """
    foreign = await driver.create_device("X")
    await foreign.create_control("temp", ControlType.VALUE, value=1)
    watched = await client.get_device("X")
    events: list[str] = []
    watched.on_added(lambda device: events.append("added"))
    watched.on_removed(lambda device: events.append("removed"))
    watched.on_control_added(lambda control: events.append(f"{control.name} added"))
    watched.on_control_removed(lambda control: events.append(f"{control.name} removed"))
    await foreign.remove()
    await wait_until(lambda: "removed" in events)
    own = await client.create_device("X")
    level = await own.create_control("level", ControlType.VALUE, value=0)
    await wait_until(lambda: "level added" in events)
    events.clear()

    await level.set_value(1)
    await level.set_error(ControlErrorFlags.READ)
    await own.set_error(ControlErrorFlags.WRITE)
    await own.remove()

    await asyncio.sleep(0.5)
    assert sorted(events) == ["level removed", "removed"]
    assert not watched.exists and not level.exists


async def test_wait_value_with_a_function(client: Client, level: Control) -> None:
    """
    The function is checked on every new value. Its exception reaches the caller, also for the current value.
    """

    def broken(value: Any) -> bool:
        if value == 3:
            raise RuntimeError("broken condition")
        return False

    above_five = asyncio.ensure_future(level.wait_value(lambda value: value > 5, timeout=5))
    broken_wait = asyncio.ensure_future(level.wait_value(broken, timeout=5))
    await wait_until(lambda: internals(client)["waits"] == 2)
    for value in range(1, 11):
        await level.set_value(value)

    assert await wait_or_dump(above_five, timeout=2) == 6
    with pytest.raises(RuntimeError):
        await wait_or_dump(broken_wait, timeout=2)
    with pytest.raises(ZeroDivisionError):
        await level.wait_value(lambda value: value / 0, timeout=1)
    assert internals(client)["waits"] == 0
