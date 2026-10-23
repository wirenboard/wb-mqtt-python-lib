"""
Several clients in one process at once: each sees the devices of the others as foreign.
"""

import asyncio
import json
from typing import Any

import pytest

from tests.broker import (
    MosquittoBroker,
    device_object,
    read_retained,
    wait_or_dump,
    wait_until,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import Client, Control, ControlType, Device, DeviceTakenError


@pytest.mark.timeout(150)
async def test_two_clients_mirror_each_other_both_ways(connect: ConnectClient) -> None:
    """
    Each client copies every value of a control of the other into its own control and reads the copy back.

    Both directions run at once, every value comes back in order.
    """
    first = await connect("service-a")
    second = await connect("service-b")

    async def build(
        source_client: Client, mirror_client: Client, source_name: str, mirror_name: str
    ) -> tuple[Control, list[int]]:
        source = await (await source_client.create_device(source_name)).create_control(
            "level", ControlType.VALUE, value=0
        )
        mirror = await (await mirror_client.create_device(mirror_name)).create_control(
            "level", ControlType.VALUE, value=0
        )
        remote_source = await mirror_client.get_control(f"{source_name}/level")

        async def copy(_control: Control, value: Any, _previous: Any) -> None:
            await mirror.set_value(value)

        remote_source.on_value(copy)
        remote_mirror = await source_client.get_control(f"{mirror_name}/level")
        seen: list[int] = []
        remote_mirror.on_value(lambda control, value, previous: seen.append(value))
        return source, seen

    (source_a, seen_a), (source_b, seen_b) = await wait_or_dump(
        asyncio.gather(
            build(first, second, "a_source", "b_mirror"), build(second, first, "b_source", "a_mirror")
        ),
        timeout=10,
    )

    async def burst(source: Control) -> None:
        for value in range(1, 201):
            await source.set_value(value)

    # The bursts are slow on a single-core WB6 in asyncio debug mode
    await wait_or_dump(asyncio.gather(burst(source_a), burst(source_b)), timeout=100)
    await wait_until(lambda: seen_a[-1:] == [200] and seen_b[-1:] == [200], timeout=30)
    assert seen_a == list(range(201)) and seen_b == list(range(201))


async def test_same_name_created_by_two_clients_at_once_ends_with_an_owner(
    broker: MosquittoBroker, connect: ConnectClient
) -> None:
    """
    Two clients create the same device in the same moment, ten times over.

    Each call ends with a device or DeviceTakenError. Both may pass the check and both own the name then,
    own devices are not followed. The broker keeps the meta of an owner.
    """
    first = await connect("service-a")
    second = await connect("service-b")
    names = [f"X{index}" for index in range(10)]

    for name in names:
        results = await wait_or_dump(
            asyncio.gather(first.create_device(name), second.create_device(name), return_exceptions=True),
            timeout=5,
        )
        assert all(isinstance(result, (Device, DeviceTakenError)) for result in results), results

    for name in names:
        owners = {client.driver for client in (first, second) if device_object(client, name).is_own}
        retained = await read_retained(broker.port, f"/devices/{name}/meta")
        assert json.loads(retained[f"/devices/{name}/meta"])["driver"] in owners, name


async def test_handlers_of_two_clients_wait_for_each_other(connect: ConnectClient) -> None:
    """
    A command handler of one client waits for a confirmation that comes from a handler of another client,
    and that handler first reads a device of the first client.
    """
    first = await connect("service-a")
    second = await connect("service-b")
    device_a = await first.create_device("a")
    await device_a.create_control("state", ControlType.VALUE, value=1)
    trigger = await device_a.create_control("trigger", ControlType.SWITCH, value=False)
    relay = await (await second.create_device("b")).create_control("relay", ControlType.SWITCH, value=False)

    async def relay_command(control: Control, value: Any) -> None:
        state = await second.get_control("a/state", timeout=5)
        await state.wait_value(lambda current: current is not None, timeout=5)
        await control.set_value(value)

    async def trigger_command(control: Control, value: Any) -> None:
        remote_relay = await first.get_control("b/relay", timeout=5)
        await remote_relay.set_value(value)
        await remote_relay.wait_value(value, timeout=5)
        await control.set_value(value)

    relay.on_command(relay_command)
    trigger.on_command(trigger_command)
    user = await connect("user")
    remote_trigger = await user.get_control("a/trigger", timeout=5)

    await remote_trigger.set_value(True)

    assert await wait_or_dump(remote_trigger.wait_value(True, timeout=10), timeout=11) is True
    assert relay.value is True
