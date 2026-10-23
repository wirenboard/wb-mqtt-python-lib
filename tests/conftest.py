"""
Shared fixtures. Every test gets its own mosquitto, asyncio runs in debug mode.
"""

# pytest injects fixtures by argument name
# pylint: disable=redefined-outer-name

import os
from pathlib import Path
from typing import AsyncIterator, Awaitable, Callable, Iterator

import pytest

from tests.broker import DeadLinkProxy, MosquittoBroker, wait_or_dump
from wb_mqtt_lib import Client, Control, ControlType
from wb_mqtt_lib import client as client_module

# Debug mode logs callbacks that block the loop too long and coroutines that were never awaited.
# The variable is read when an event loop is created, so it is set before pytest-asyncio creates one.
os.environ.setdefault("PYTHONASYNCIODEBUG", "1")

ConnectClient = Callable[..., Awaitable[Client]]


@pytest.fixture(autouse=True)
def free_drivers() -> Iterator[None]:
    """
    A test that fails before it closes a client of its own leaves the driver taken, and the next tests
    would fail on that instead of on their own checks.
    """
    yield
    client_module._DRIVERS_IN_USE.clear()  # pylint: disable=protected-access


@pytest.fixture
def broker(tmp_path: Path) -> Iterator[MosquittoBroker]:
    """
    Private mosquitto for one test.
    """
    with MosquittoBroker(tmp_path) as instance:
        yield instance


@pytest.fixture
async def connect(broker: MosquittoBroker) -> AsyncIterator[ConnectClient]:
    """
    Factory of connected clients, all closed after the test.
    """
    clients = []

    async def factory(driver: str = "test-service", transport: str = "tcp") -> Client:
        client = Client(driver, broker_url=broker.url(transport))
        await wait_or_dump(client.connect(), timeout=5)
        clients.append(client)
        return client

    yield factory
    for client in reversed(clients):
        await wait_or_dump(client.close(), timeout=5)


@pytest.fixture
async def client(connect: ConnectClient) -> Client:
    """
    The service under test, driver «test-service».
    """
    return await connect()


@pytest.fixture
async def driver(connect: ConnectClient) -> Client:
    """
    Another service on the same broker. It publishes the devices the service under test reads.
    """
    return await connect("driver")


@pytest.fixture
async def level(client: Client) -> Control:
    """
    Own control dev/level of the service under test, of type value with the value 0.
    """
    return await (await client.create_device("dev")).create_control("level", ControlType.VALUE, value=0)


@pytest.fixture
async def dead_link(broker: MosquittoBroker) -> AsyncIterator[tuple[DeadLinkProxy, Client]]:
    """
    A client connected through DeadLinkProxy, which plays a link that is dead but not closed yet.
    """
    proxy = DeadLinkProxy(broker.port)
    await proxy.start()
    client = Client("test-service", proxy.url())
    await wait_or_dump(client.connect(), timeout=5)
    yield proxy, client
    await wait_or_dump(client.close(), timeout=10)
    await proxy.close()
