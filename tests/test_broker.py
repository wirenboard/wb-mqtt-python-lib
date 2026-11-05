"""
Checks that the helpers work before the library tests rely on them: the broker fixture and the deadlock
detector.
"""

import asyncio

import aiomqtt
import pytest

from tests.broker import (
    HOST,
    DeadlockSuspectedError,
    MosquittoBroker,
    cancel_and_wait,
    read_retained,
    wait_or_dump,
)


async def test_broker_keeps_retained_until_restart(broker: MosquittoBroker) -> None:
    """
    The broker on the controller runs without persistence, so a restart drops every retained message.

    Reconnect handling in the library relies on this, and the fixture must behave the same way.
    """
    async with aiomqtt.Client(HOST, broker.port) as client:
        await client.publish("/devices/test/controls/K1", "1", qos=1, retain=True)
    assert await read_retained(broker.port, "/devices/test/#") == {"/devices/test/controls/K1": b"1"}

    await asyncio.to_thread(broker.restart)

    assert await read_retained(broker.port, "/devices/test/#") == {}


async def ping_pong(port: int, handler_in_own_task: bool) -> bytes:
    """
    The handler of "ping" publishes "pong" and waits until the read loop receives it.

    With the handler awaited inside the read loop nobody reads "pong", that is the deadlock the library
    avoids. With the handler in its own task, the read loop keeps reading and "pong" arrives.
    """
    async with aiomqtt.Client(HOST, port) as client:
        await client.subscribe("test/#")
        loop = asyncio.get_running_loop()
        pong = loop.create_future()
        handled = loop.create_future()
        handler_tasks = set()

        async def on_ping() -> None:
            await client.publish("test/pong", "pong")
            handled.set_result(await pong)

        async def read_loop() -> None:
            async for message in client.messages:
                if message.topic.matches("test/pong"):
                    pong.set_result(message.payload)
                elif handler_in_own_task:
                    handler_tasks.add(asyncio.create_task(on_ping()))
                else:
                    await on_ping()

        reader = asyncio.create_task(read_loop())
        try:
            await client.publish("test/ping", "ping")
            return await wait_or_dump(handled, timeout=2)
        finally:
            for task in (reader, *handler_tasks):
                await cancel_and_wait(task)


async def test_detector_reports_a_handler_awaited_in_the_read_loop(broker: MosquittoBroker) -> None:
    """
    A handler awaited inside the read loop hangs, and the detector turns that into a failure naming it.
    """
    with pytest.raises(DeadlockSuspectedError, match="on_ping"):
        await ping_pong(broker.port, handler_in_own_task=False)


async def test_detector_lets_the_same_handler_in_its_own_task_through(broker: MosquittoBroker) -> None:
    """
    The same handler in its own task gets the reply, so the failure above comes from the awaiting,
    not from the detector.
    """
    assert await ping_pong(broker.port, handler_in_own_task=True) == b"pong"
