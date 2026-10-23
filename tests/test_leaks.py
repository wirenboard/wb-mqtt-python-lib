"""
Leaks: repeated work must come back to the same counts and the same live objects, a closed client must go.

Live objects are counted by type with the garbage collector over two windows of a hundred cycles after
a warm-up. A leak is at least one object per cycle and grows in both windows. A one-off cache fills in one
window at most, for example the cache of isinstance checks against abstract base classes.
Two types are left out. A cancelled asyncio timeout keeps its TimerHandle with a Context in the loop until
its deadline, for the end of retained delivery that is 30 s, so their number follows the pace of the cycles
and drops back by itself. asyncio debug mode is off during the measurement, it keeps a traceback for every
callback.
"""

import asyncio
import gc
import weakref
from collections import Counter
from typing import Any, Awaitable, Callable

import pytest

from tests.broker import (
    MosquittoBroker,
    control_object,
    internals,
    wait_or_dump,
    wait_until,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import Client, Control, ControlType, Message

WARM_UP_CYCLES = 20
MEASURED_CYCLES = 100
# Types that may grow by a few objects without a leak, for example the cache of the struct module
ALLOWED_GROWTH = MEASURED_CYCLES // 10
TIMER_TYPES = frozenset({"TimerHandle", "Context"})


def live_objects() -> Counter:
    """
    Objects the garbage collector tracks, by type.
    """
    gc.collect()
    return Counter(
        type(item).__qualname__ for item in gc.get_objects() if type(item).__qualname__ not in TIMER_TYPES
    )


async def object_growth(cycle: Callable[[int], Awaitable[None]]) -> dict[str, int]:
    """
    Types whose number of live objects grew in both measured windows, with the growth of the second one.
    """
    loop = asyncio.get_running_loop()
    debug = loop.get_debug()
    loop.set_debug(False)
    windows = []
    try:
        for index in range(WARM_UP_CYCLES):
            await cycle(index)
        for window in range(2):
            before = live_objects()
            start = WARM_UP_CYCLES + window * MEASURED_CYCLES
            for index in range(start, start + MEASURED_CYCLES):
                await cycle(index)
            after = live_objects()
            windows.append({name: after[name] - before[name] for name in after})
    finally:
        loop.set_debug(debug)
    first, second = windows[0], windows[1]
    return {
        name: growth
        for name, growth in second.items()
        if growth > ALLOWED_GROWTH and first.get(name, 0) > ALLOWED_GROWTH
    }


async def test_closed_client_is_garbage_collected(broker: MosquittoBroker) -> None:
    """
    Nothing in the library keeps a closed client alive, and no task of it is left running.
    """
    tasks_before = len(asyncio.all_tasks())
    references = []
    for _ in range(5):
        client = Client("test-service", broker.url())
        await wait_or_dump(client.connect(), timeout=5)
        device = await client.create_device("dev")
        level = await device.create_control("level", ControlType.VALUE, value=0)
        level.on_value(lambda control, value, previous: None)
        await client.raw.subscribe("/raw/#", lambda message: None)
        await client.get_control("missing/ctl")
        client.start_task(asyncio.sleep(60), "background work")
        await level.set_value(1)
        await wait_or_dump(client.close(), timeout=5)
        references.append(weakref.ref(client))
        del client, device, level

    await asyncio.sleep(0)
    gc.collect()
    assert [reference() for reference in references] == [None] * 5
    assert len(asyncio.all_tasks()) == tasks_before


async def test_create_and_remove_cycles_do_not_grow(connect: ConnectClient, client: Client) -> None:
    """
    A device is created, read by another client, removed, and so on with the same names.
    """
    reader = await connect("reader")

    def handler(_control: Control, _value: Any, _previous: Any) -> None:
        pass

    async def cycle(index: int) -> None:
        device = await client.create_device("dev")
        level = await device.create_control("level", ControlType.VALUE, value=index)
        level.on_value(handler)
        remote = await reader.get_control("dev/level")
        remote.on_value(handler)
        await remote.wait_value(index, timeout=5)
        await device.remove()
        await wait_until(lambda: not remote.exists)

    growth = await wait_or_dump(object_growth(cycle), timeout=120)
    client_counts, reader_counts = internals(client), internals(reader)
    await wait_or_dump(cycle(0), timeout=10)

    assert not growth
    assert (internals(client), internals(reader)) == (client_counts, reader_counts)


async def test_raw_subscribe_read_and_unsubscribe_cycles_leave_nothing(client: Client) -> None:
    """
    A new raw handler per cycle, a one-off read and removal of the handler.
    """

    async def cycle(index: int) -> None:
        received = asyncio.get_running_loop().create_future()

        def handler(message: Message) -> None:
            if not received.done():
                received.set_result(message)

        await client.raw.subscribe(f"/leak/{index % 5}/#", handler)
        await client.raw.publish(f"/leak/{index % 5}/value", str(index), retain=True)
        await wait_or_dump(received, 5)
        await client.raw.read("/leak/#")
        await client.raw.unsubscribe(handler)

    growth = await wait_or_dump(object_growth(cycle), timeout=120)

    assert not growth
    assert internals(client) == {
        "subscriptions": 0,
        # own retained raw publications, kept for the replay after reconnect
        "replay mirror": 5,
        "listeners": 0,
        "service tasks": 0,
        "internal tasks": 0,
        "waits": 0,
    }


async def test_waits_that_time_out_leave_nothing(client: Client, level: Control) -> None:
    """
    wait_value, get_control and get_device that give up by timeout.
    """

    async def cycle(_index: int) -> None:
        with pytest.raises(TimeoutError):
            await level.wait_value(-1, timeout=0.001)
        with pytest.raises(TimeoutError):
            await client.get_control("missing/ctl", timeout=0.001)
        with pytest.raises(TimeoutError):
            await client.get_device("missing_dev", timeout=0.001)

    growth = await wait_or_dump(object_growth(cycle), timeout=60)

    assert not growth
    assert internals(client)["waits"] == 0


async def test_failing_handlers_leave_nothing(client: Client, level: Control) -> None:
    """
    Handlers and background tasks that raise are logged and leave no task behind.
    """
    calls = []

    def failing(_control: Control, value: Any, _previous: Any) -> None:
        calls.append(value)
        raise RuntimeError("handler failed")

    async def failing_task() -> None:
        raise RuntimeError("task failed")

    level.on_value(failing)
    listeners_before = internals(client)["listeners"]
    for value in range(1, 101):
        await level.set_value(value)
        client.start_task(failing_task(), "failing work")

    await wait_until(lambda: calls[-1:] == [100] and internals(client)["service tasks"] == 0)
    assert internals(client)["listeners"] == listeners_before


async def test_handlers_and_waits_on_ever_new_controls_leave_nothing(driver: Client, client: Client) -> None:
    """
    Under a new name each time, a reader takes its handler off a control after the control is gone and waits
    for a control that never comes. No record of these controls stays.
    """
    device = await driver.create_device("dev")
    await client.get_device("dev")

    def handler(_control: Control, _value: Any, _previous: Any) -> None:
        pass

    async def cycle(index: int) -> None:
        own = await device.create_control(f"c{index}", ControlType.VALUE, value=1)
        remote = control_object(client, f"dev/c{index}")
        remote.on_value(handler)
        await own.remove()
        await wait_until(lambda: not remote.exists)
        remote.remove_handler(handler)
        with pytest.raises(TimeoutError):
            await control_object(client, f"dev/missing{index}").wait_value(1, timeout=0.001)
        with pytest.raises(TimeoutError):
            await client.get_control(f"dev/absent{index}", timeout=0.001)

    growth = await wait_or_dump(object_growth(cycle), timeout=120)

    assert not growth


async def test_values_and_errors_without_meta_under_new_names_leave_nothing(
    driver: Client, client: Client
) -> None:
    """
    A driver publishes values without meta under ever new control names and erases them, and errors without
    the letters r, w and p. The reader keeps no record for any of them.
    """
    known = await (await driver.create_device("dev")).create_control("known", ControlType.VALUE, value=-1)
    remote = await client.get_control("dev/known")

    async def cycle(index: int) -> None:
        topic = f"/devices/dev/controls/n{index}"
        await driver.raw.publish(topic, "1", retain=True)
        await driver.raw.publish(topic, "", retain=True)
        await driver.raw.publish(f"{topic}/meta/error", "timeout")
        # Messages of one device arrive in order, so the erasure is applied once this value is
        await known.set_value(index)
        await remote.wait_value(index, timeout=5)

    growth = await wait_or_dump(object_growth(cycle), timeout=120)

    assert not growth
