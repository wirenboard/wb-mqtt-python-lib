"""
Lifecycle: close from a handler, a task or outside, calls after close, waits that end on close or a lost link,
cancelled connect and close.
"""

import asyncio
import time
from typing import Any

import pytest

from tests.broker import (
    BROKER_READS,
    FIRST_VALUE_META,
    HOST,
    DeadLinkProxy,
    MosquittoBroker,
    Send,
    ack_timeout,
    cancel_and_wait,
    intercept_publish,
    internals,
    lose_end_markers,
    silent_server,
    wait_for_task_count,
    wait_or_dump,
    wait_retained,
    wait_until,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import (
    BrokerDisconnectedError,
    Client,
    ClientClosedError,
    Control,
    ControlErrorFlags,
    ControlType,
)
from wb_mqtt_lib._mqtt import subscriber as subscriber_module

# A raw subscription waits for the broker like the reads and must not stay as subscribed after close
DEAD_LINK_WAITS = {**BROKER_READS, "raw.subscribe": lambda client: client.raw.subscribe("/x/#", print)}


async def test_close_from_a_command_handler(connect: ConnectClient, client: Client) -> None:
    """
    A service stops itself from a command handler. close cancels everything but the handler running it.
    """
    stop = await (await client.create_device("dev")).create_control("stop", ControlType.PUSHBUTTON)
    level = await (await client.create_device("other")).create_control("level", ControlType.VALUE, value=0)
    level.on_value(lambda control, value, previous: None)
    client.start_task(asyncio.sleep(60), "background work")
    closed = asyncio.get_running_loop().create_future()

    async def on_stop(_control: Control, _value: Any) -> None:
        await client.close()
        closed.set_result(client.is_connected)

    stop.on_command(on_stop)
    user = await connect("user")
    await user.raw.publish("/devices/dev/controls/stop/on", "1")

    assert await wait_or_dump(closed, timeout=5) is False
    await wait_until(lambda: internals(client)["listeners"] == 0 and internals(client)["service tasks"] == 0)


async def test_close_from_a_background_task_and_from_outside_at_once(client: Client) -> None:
    """
    Two close calls at once, one of them inside a background task of the client, both return.
    """
    await (await client.create_device("dev")).create_control("level", ControlType.VALUE, value=0)

    inside = client.start_task(client.close(), "close inside")
    await wait_or_dump(asyncio.gather(inside, client.close()), timeout=5)

    assert (
        not client.is_connected
        and internals(client)["service tasks"] == 0
        and internals(client)["listeners"] == 0
    )


async def test_close_cancels_tasks_and_handlers_that_wait_for_the_broker(
    driver: Client, client: Client
) -> None:
    """
    close() cancels a background task and a handler of the client while they wait for a value, also a loop
    that catches Exception around its wait, and nothing of the client keeps running. The wait of other code
    ends with ClientClosedError.
    """
    await (await driver.create_device("dev")).create_control("level", ControlType.VALUE, value=1)
    level = await client.get_control("dev/level")
    started = asyncio.Event()
    caught: list[Exception] = []

    def never(_value: object) -> bool:
        return False

    async def waiting_handler(control: Control, _value: Any, _previous: Any) -> None:
        started.set()
        await control.wait_value(never, timeout=60)

    async def poll_forever() -> None:
        while True:
            try:
                await level.wait_value(never, timeout=60)
            except Exception as error:  # pylint: disable=broad-exception-caught
                caught.append(error)

    level.on_value(waiting_handler)
    waiting = client.start_task(level.wait_value(never, timeout=60))
    looping = client.start_task(poll_forever())
    outside = asyncio.ensure_future(level.wait_value(never, timeout=60))
    await wait_or_dump(started.wait(), timeout=2)
    await asyncio.sleep(0.1)

    await wait_or_dump(client.close(), timeout=3)

    assert waiting.cancelled() and looping.cancelled() and not caught
    assert internals(client)["listeners"] == 0 and internals(client)["service tasks"] == 0
    assert not client.is_connected
    with pytest.raises(ClientClosedError):
        await wait_or_dump(outside, timeout=2)


async def test_close_wakes_pending_waits(client: Client, level: Control) -> None:
    """
    Waits for a value or for appearance end with ClientClosedError instead of their timeout.
    """
    waits = [
        asyncio.ensure_future(level.wait_value(5, timeout=30)),
        asyncio.ensure_future(client.get_control("missing/ctl", timeout=30)),
        asyncio.ensure_future(client.get_device("missing_dev", timeout=30)),
    ]
    await wait_until(lambda: internals(client)["waits"] == 3)

    await wait_or_dump(client.close(), timeout=3)

    for wait in waits:
        with pytest.raises(ClientClosedError):
            await wait_or_dump(wait, timeout=2)


async def test_calls_after_close_raise_instead_of_hanging(driver: Client, client: Client) -> None:
    """
    After close writing, waiting and new listeners raise ClientClosedError. Fields keep the last state.
    Handlers still come off, a raw filter leaves the client with its last handler.
    """
    await (await driver.create_device("other")).create_control("x", ControlType.VALUE, value=1)
    device = await client.create_device("dev")
    temp = await device.create_control("temp", ControlType.VALUE, value=1)
    foreign = await client.get_control("other/x")
    raw_handler = await client.raw.subscribe("/raw/#", lambda message: None)
    control_handler = foreign.on_value(lambda control, value, previous: None)
    connection_handler = client.on_connection_change(lambda connected: None)
    subscriptions = internals(client)["subscriptions"]

    await wait_or_dump(client.close(), timeout=5)

    await wait_or_dump(client.raw.unsubscribe(raw_handler), timeout=2)
    foreign.remove_handler(control_handler)
    client.remove_handler(connection_handler)
    assert internals(client)["subscriptions"] == subscriptions - 1

    calls = [
        temp.set_value(2),
        foreign.set_value(2),
        temp.set_error(ControlErrorFlags.READ),
        temp.update_meta(title="New"),
        device.remove(),
        device.create_control("new", ControlType.VALUE),
        client.create_device("new"),
        client.get_control("other/x"),
        client.get_device_names(),
        client.remove_driver_devices(),
        foreign.wait_value(1, timeout=1),
        client.raw.publish("/x", "1"),
        client.raw.read("/x"),
        client.raw.subscribe("/x", print),
    ]
    for call in calls:
        with pytest.raises(ClientClosedError):
            await wait_or_dump(call, timeout=2)
    for add_listener in (temp.on_value, device.on_added, client.on_connection_change):
        with pytest.raises(ClientClosedError):
            add_listener(print)
    with pytest.raises(ClientClosedError):
        client.start_task(asyncio.sleep(1))
    with pytest.raises(ClientClosedError):
        await client.connect()
    assert temp.value == 1 and foreign.value == 1 and temp.exists and device.is_own


async def test_second_client_with_the_same_driver_is_refused(connect: ConnectClient) -> None:
    """
    A second client with the same driver is refused until the first one is closed. A connected client
    refuses a second connect().
    """
    first = await connect("wb-mqtt-test")
    await connect("wb-mqtt-other")

    with pytest.raises(ValueError):
        await connect("wb-mqtt-test")
    with pytest.raises(RuntimeError):
        await first.connect()

    await first.close()
    assert (await connect("wb-mqtt-test")).is_connected


async def test_async_with_connects_and_closes(broker: MosquittoBroker) -> None:
    """
    async with connects the client and closes it on the way out. The devices stay in the broker.
    """
    async with Client("test-service", broker.url()) as client:
        assert client.is_connected
        await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=21)

    assert not client.is_connected
    with pytest.raises(ClientClosedError):
        await client.create_device("other")
    await wait_retained(
        broker.port,
        "/devices/dev/#",
        {
            "/devices/dev/meta": b'{"driver": "test-service"}',
            "/devices/dev/controls/temp/meta": FIRST_VALUE_META,
            "/devices/dev/controls/temp": b"21",
        },
    )


async def test_cancelled_connect_leaves_no_tasks_and_frees_the_driver(broker: MosquittoBroker) -> None:
    """
    The broker is down and the service gives up waiting. The client stops retrying in the background.
    """
    await asyncio.to_thread(broker.stop)
    tasks_before = len(asyncio.all_tasks())
    client = Client("test-service", broker.url())

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(client.connect(), 1.0)

    await wait_for_task_count(tasks_before)
    await asyncio.to_thread(broker.start)
    second = Client("test-service", broker.url())
    await wait_or_dump(second.connect(), timeout=5)
    await wait_or_dump(second.close(), timeout=5)


async def test_connection_handler_gets_no_false_before_the_first_connection(broker: MosquittoBroker) -> None:
    """
    A handler registered before connect() hears nothing while the broker is down, then True once
    the client connects.
    """
    await asyncio.to_thread(broker.stop)
    client = Client("test-service", broker.url())
    changes: list[bool] = []
    client.on_connection_change(changes.append)
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(client.connect(), 1.0)
        await asyncio.sleep(0.2)
        assert not changes

        await asyncio.to_thread(broker.start)
        await wait_or_dump(client.connect(), timeout=5)

        await wait_until(lambda: changes == [True])
    finally:
        await wait_or_dump(client.close(), timeout=10)


async def test_close_while_connect_retries_ends_connect_with_client_closed(broker: MosquittoBroker) -> None:
    """
    The broker is down, another task closes the client while connect retries.

    connect raises ClientClosedError instead of returning as if it had connected.
    """
    await asyncio.to_thread(broker.stop)
    client = Client("test-service", broker.url())
    attempt = asyncio.ensure_future(client.connect())
    await asyncio.sleep(0.3)

    await wait_or_dump(client.close(), timeout=5)

    with pytest.raises(ClientClosedError):
        await wait_or_dump(attempt, timeout=5)


async def test_connect_given_up_while_the_broker_is_silent_ends_on_time_and_frees_the_driver(
    broker: MosquittoBroker,
) -> None:
    """
    The broker took the connection and does not answer. wait_for gives up on connect on time, and the driver
    name is free at once. The attempt ends by its own timeout, close() waits for that.
    """
    # Reads until either side closes the connection
    async with silent_server(lambda reader: reader.read()) as port:
        tasks_before = len(asyncio.all_tasks())
        stuck = Client("test-service", f"tcp://{HOST}:{port}")
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(stuck.connect(), 0.3)
        # Long before the CONNACK timeout of the attempt, 5 s
        assert time.monotonic() - started < 2

        second = Client("test-service", broker.url())
        await wait_or_dump(second.connect(), timeout=5)
        await wait_or_dump(second.close(), timeout=5)
        await wait_or_dump(stuck.close(), timeout=10)
        # The server still holds the connection, only the CONNACK timeout of the attempt has ended it
        assert not [task for task in asyncio.all_tasks() if task.get_name().startswith("mqtt sessions")]
    # Handlers of the silent server end a few loop iterations after their connections close
    await wait_for_task_count(tasks_before)


async def test_connect_right_after_a_cancelled_one_waits_until_its_attempt_ends(
    broker: MosquittoBroker,
) -> None:
    """
    connect() is cancelled while the broker does not answer and is called again at once. The new call waits
    until the attempt of the cancelled one ends, so one session loop runs. Two would share the stop flag
    and the subscriptions and kick each other out with one client id.
    """
    proxy = DeadLinkProxy(broker.port)
    await proxy.start()
    proxy.dead = True
    client = Client("test-service", proxy.url())
    try:
        attempt = asyncio.ensure_future(client.connect())
        await asyncio.sleep(0.3)
        await cancel_and_wait(attempt)
        proxy.dead = False

        await wait_or_dump(client.connect(), timeout=10)

        loops = [task for task in asyncio.all_tasks() if task.get_name().startswith("mqtt sessions")]
        assert client.is_connected and len(loops) == 1
    finally:
        await wait_or_dump(client.close(), timeout=10)
        await proxy.close()


@pytest.mark.parametrize("call", DEAD_LINK_WAITS)
async def test_close_ends_a_broker_read_on_a_dead_link_with_client_closed(
    dead_link: tuple[DeadLinkProxy, Client], call: str
) -> None:
    """
    A call that reads the broker waits for SUBACK on a link that is dead but not closed yet. close() ends it
    with ClientClosedError, within the 5 s of that wait, not with BrokerDisconnectedError. A raw
    subscription does not stay as if it were subscribed.
    """
    proxy, client = dead_link
    proxy.dead = True
    pending = asyncio.ensure_future(DEAD_LINK_WAITS[call](client))
    await asyncio.sleep(0.2)

    await wait_or_dump(client.close(), timeout=10)

    with pytest.raises(ClientClosedError):
        await wait_or_dump(pending, timeout=6)


@pytest.mark.parametrize("end", ["close", "link lost", "silent broker"])
@pytest.mark.parametrize("call", BROKER_READS)
async def test_wait_for_the_broker_ends_on_close_link_loss_or_silence(
    broker: MosquittoBroker, client: Client, monkeypatch: pytest.MonkeyPatch, call: str, end: str
) -> None:
    """
    A call that reads the broker waits for the end of retained delivery. Close ends it at once with
    ClientClosedError and a lost link with BrokerDisconnectedError. A broker that does not answer
    in RETAINED_END_TIMEOUT_S counts as a lost link too.
    """
    lose_end_markers(client, monkeypatch)
    if end == "silent broker":
        monkeypatch.setattr(subscriber_module, "RETAINED_END_TIMEOUT_S", 0.5)
    pending = asyncio.ensure_future(BROKER_READS[call](client))
    await asyncio.sleep(0.2)
    assert not pending.done()

    if end == "close":
        await wait_or_dump(client.close(), timeout=5)
    elif end == "link lost":
        await asyncio.to_thread(broker.stop)

    with pytest.raises(ClientClosedError if end == "close" else BrokerDisconnectedError):
        await wait_or_dump(pending, timeout=2)


async def test_close_cancelled_midway_is_finished_by_the_next_close(broker: MosquittoBroker) -> None:
    """
    The task that closes the client is cancelled while close() waits for a background task that ends
    slowly. The cleanup goes on, the next close() waits for it, and the driver is free after it.
    """
    client = Client("test-service", broker.url())
    await wait_or_dump(client.connect(), timeout=5)

    async def slow_to_end() -> None:
        try:
            await asyncio.sleep(60)
        finally:
            await asyncio.sleep(0.3)

    client.start_task(slow_to_end())
    closing = asyncio.ensure_future(client.close())
    await asyncio.sleep(0.1)
    await cancel_and_wait(closing)

    await wait_or_dump(client.close(), timeout=5)

    assert not client.is_connected
    second = Client("test-service", broker.url())
    await wait_or_dump(second.connect(), timeout=5)
    await wait_or_dump(second.close(), timeout=5)


@pytest.mark.parametrize("where", ["background task", "handler"])
async def test_close_inside_a_task_that_close_cancels_does_not_wait_for_itself(
    client: Client, where: str
) -> None:
    """
    A background task or a handler calls close() in its finally. close() from outside cancels it, and
    the two return instead of waiting for each other.
    """
    started = asyncio.Event()

    async def work(*_args: Any) -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            await client.close()

    if where == "background task":
        client.start_task(work())
    else:
        level = await (await client.create_device("dev")).create_control("level", ControlType.VALUE, value=0)
        level.on_value(work)
    await wait_or_dump(started.wait(), timeout=2)

    await wait_or_dump(client.close(), timeout=5)

    assert not client.is_connected


@pytest.mark.parametrize("call", ["remove", "create_device", "update_meta", "create_control", "raw.publish"])
async def test_close_ends_a_change_waiting_for_the_broker_with_client_closed(
    connect: ConnectClient, monkeypatch: pytest.MonkeyPatch, call: str
) -> None:
    """
    close() comes while a change waits for the broker to confirm it. That is remove(), create_device over
    leftovers of a previous run, update_meta, create_control or an event. The call ends with
    ClientClosedError, like every wait for the broker on close.
    """
    previous_run = await connect("service")
    device = await previous_run.create_device("X")
    control = await device.create_control("c", ControlType.VALUE, value=1)
    client = previous_run
    if call == "create_device":
        await previous_run.close()
        client = await connect("service")
    held: list[str] = []
    unconfirmed = ("/devices/X/controls/c/meta", "/devices/X/controls/new/meta", "/events/x")

    async def never_confirm(send: Send, topic: str, payload: Any) -> None:
        await send()
        if payload is None or topic in unconfirmed:
            held.append(topic)
            await asyncio.Event().wait()

    intercept_publish(monkeypatch, never_confirm)
    calls = {
        "remove": device.remove,
        "create_device": lambda: client.create_device("X"),
        "update_meta": lambda: control.update_meta(units="V"),
        "create_control": lambda: device.create_control("new", ControlType.VALUE),
        "raw.publish": lambda: client.raw.publish("/events/x", "1"),
    }
    pending = asyncio.ensure_future(calls[call]())
    await wait_until(lambda: bool(held))

    await wait_or_dump(client.close(), timeout=5)

    with pytest.raises(ClientClosedError):
        await wait_or_dump(pending, timeout=2)


async def test_close_ends_a_change_still_queued_behind_an_erasure_with_client_closed(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    update_meta waits in the queue behind a stage of an erasure the broker does not confirm, and close()
    comes. The call ends with ClientClosedError, though its message never went to the connection.
    """
    device = await client.create_device("dev")
    control = await device.create_control("c", ControlType.VALUE, value=1)
    other = await client.create_device("other")
    await other.create_control("c", ControlType.VALUE, value=1)
    held: list[str] = []

    async def never_confirm_an_erasure(send: Send, topic: str, payload: Any) -> None:
        await send()
        if payload is None:
            held.append(topic)
            await asyncio.Event().wait()

    intercept_publish(monkeypatch, never_confirm_an_erasure)
    removal = asyncio.ensure_future(other.remove())
    await wait_until(lambda: bool(held))
    change = asyncio.ensure_future(control.update_meta(units="V"))
    await asyncio.sleep(0.1)
    assert not change.done()

    await wait_or_dump(client.close(), timeout=5)

    for call in (change, removal):
        with pytest.raises(ClientClosedError):
            await wait_or_dump(call, timeout=2)


async def test_retained_change_timed_out_after_close_began_ends_with_client_closed(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    update_meta waits for PUBACK, close() begins, then the wait runs out while the link lives. The call ends
    with ClientClosedError and does not return as if its meta went out again.
    """
    control = await (await client.create_device("dev")).create_control("c", ControlType.VALUE, value=1)

    async def slow_to_end() -> None:
        try:
            await asyncio.sleep(60)
        finally:
            await asyncio.sleep(0.5)

    client.start_task(slow_to_end())
    held = asyncio.Event()
    release = asyncio.Event()

    async def time_out_the_meta_once(send: Send, topic: str, _payload: Any) -> None:
        if topic == "/devices/dev/controls/c/meta" and not held.is_set():
            held.set()
            await release.wait()
            raise ack_timeout()
        await send()

    intercept_publish(monkeypatch, time_out_the_meta_once)
    change = asyncio.ensure_future(control.update_meta(units="V"))
    await wait_or_dump(held.wait(), timeout=2)
    closing = asyncio.ensure_future(client.close())
    # close() waits for the background task to end, the session still lives
    await asyncio.sleep(0.1)
    release.set()

    with pytest.raises(ClientClosedError):
        await wait_or_dump(change, timeout=2)
    await wait_or_dump(closing, timeout=5)
