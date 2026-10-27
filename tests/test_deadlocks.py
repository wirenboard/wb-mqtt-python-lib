"""
Deadlocks: handlers that wait, publish and subscribe, races around timeouts.
"""

import asyncio
import random
from typing import Any, Awaitable, Callable

import pytest

from tests.broker import MosquittoBroker, internals, wait_or_dump, wait_until
from tests.conftest import ConnectClient
from wb_mqtt_lib import Client, Control, ControlType, Message, topics


@pytest.mark.parametrize(
    "call, expected",
    [
        ("get_control of a new device", 42),
        ("command and wait for the driver", True),
        ("direct.read", {"/config/threshold": b"7"}),
    ],
)
async def test_handler_waits_for_the_broker_inside_itself(
    driver: Client, client: Client, call: str, expected: object
) -> None:
    """
    A handler of a message from the broker waits for the broker in its turn. It subscribes to a new device,
    waits for the reply to a command or reads retained topics.
    """
    device = await driver.create_device("dev")
    trigger = await device.create_control("trigger", ControlType.VALUE, value=0)
    relay = await device.create_control("relay", ControlType.SWITCH, value=False)

    async def apply(control: Control, value: Any) -> None:
        await control.set_value(value)

    relay.on_command(apply)
    await (await driver.create_device("other")).create_control("temp", ControlType.VALUE, value=42)
    await driver.direct.publish("/config/threshold", "7", retain=True)
    remote_trigger = await client.get_control("dev/trigger")
    remote_relay = await client.get_control("dev/relay")

    async def command_and_wait() -> Any:
        await remote_relay.set_value(True)
        return await remote_relay.wait_value(True, timeout=5)

    async def get_new_device() -> Any:
        return (await client.get_control("other/temp")).value

    calls = {
        "get_control of a new device": get_new_device,
        "command and wait for the driver": command_and_wait,
        "direct.read": lambda: client.direct.read("/config/#"),
    }
    result = asyncio.get_running_loop().create_future()

    async def on_trigger(_control: Control, value: Any, _previous: Any) -> None:
        if value == 1:
            result.set_result(await calls[call]())

    remote_trigger.on_value(on_trigger)
    await trigger.set_value(1)

    assert await wait_or_dump(result, timeout=6) == expected


async def test_concurrent_subscribe_and_unsubscribe_on_one_topic(
    broker: MosquittoBroker, client: Client
) -> None:
    """
    The broker subscription stays while at least one handler is left, and is gone with the last one.

    Every handler of the second batch gets the message once, none of the first.
    """
    received: list[tuple[str, int]] = []

    def handlers(batch: str, count: int) -> list[Callable[[Message], None]]:
        return [lambda message, index=index: received.append((batch, index)) for index in range(count)]

    first_batch, second_batch = handlers("first", 20), handlers("second", 20)
    await wait_or_dump(
        asyncio.gather(*(client.direct.subscribe("/race/topic", handler) for handler in first_batch)),
        timeout=5,
    )
    await wait_or_dump(
        asyncio.gather(
            *(client.direct.subscribe("/race/topic", handler) for handler in second_batch),
            *(client.direct.unsubscribe(handler) for handler in first_batch),
        ),
        timeout=5,
    )
    await client.direct.publish("/race/topic", "x")
    await wait_until(lambda: len(received) == 20)

    await asyncio.sleep(0.2)
    assert sorted(received) == [("second", index) for index in range(20)]
    await asyncio.gather(*(client.direct.unsubscribe(handler) for handler in second_batch))
    assert internals(client)["subscriptions"] == 0
    assert broker.unsubscriptions() == ["/race/topic"]


@pytest.mark.parametrize("seed", range(3))
async def test_wait_value_racing_its_timeout(client: Client, level: Control, seed: int) -> None:
    """
    Values arrive around the timeout.

    Each wait either returns the value or raises TimeoutError, none hangs.
    """
    rng = random.Random(seed)
    outcomes = {"value": 0, "timeout": 0}

    async def one_round(target: int) -> None:
        delay = rng.uniform(0.0, 0.02)
        asyncio.get_running_loop().call_later(delay, lambda: client.start_task(level.set_value(target)))
        try:
            await level.wait_value(target, timeout=0.01)
            outcomes["value"] += 1
        except TimeoutError:
            outcomes["timeout"] += 1

    for target in range(1, 101):
        await wait_or_dump(one_round(target), timeout=2)

    assert sum(outcomes.values()) == 100
    assert internals(client)["waits"] == 0


async def test_many_handlers_get_one_new_device_at_once(
    driver: Client, client: Client, level: Control
) -> None:
    """
    Twenty handlers ask for the same new device inside their handlers, the client reads the device once.
    """
    await (await driver.create_device("other")).create_control("temp", ControlType.VALUE, value=42)
    found = []

    def make_handler() -> Callable[[Control, Any, Any], Awaitable[None]]:
        async def handler(_control: Control, value: Any, _previous: Any) -> None:
            if value == 1:
                found.append((await client.get_control("other/temp")).value)

        return handler

    for _ in range(20):
        level.on_value(make_handler())
    subscriptions_before = internals(client)["subscriptions"]
    await level.set_value(1)

    await wait_until(lambda: len(found) == 20)
    assert found == [42] * 20
    assert internals(client)["subscriptions"] == subscriptions_before + len(topics.watch_filters("other"))


async def test_command_handler_creates_and_removes_a_device(connect: ConnectClient, client: Client) -> None:
    """
    create_device waits for the broker inside a handler, the read loop keeps going meanwhile.
    """
    rebuild = await (await client.create_device("dev")).create_control("rebuild", ControlType.PUSHBUTTON)
    done = []

    async def on_rebuild(_control: Control, _value: Any) -> None:
        device = await client.create_device("built")
        await device.create_control("ch1", ControlType.VALUE, value=len(done))
        await device.remove()
        done.append(True)

    rebuild.on_command(on_rebuild)
    user = await connect("user")
    for _ in range(5):
        await user.direct.publish("/devices/dev/controls/rebuild/on", "1")

    await wait_until(lambda: len(done) == 5, timeout=10)
