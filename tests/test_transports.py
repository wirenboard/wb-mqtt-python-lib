"""
Transports, the broker URL and login: the unix socket, the default on the controller, websockets, a rejected
login at the start and after a reconnect.
"""

# pytest injects fixtures by argument name
# pylint: disable=redefined-outer-name

import asyncio
from pathlib import Path
from typing import Iterator
from urllib.parse import quote

import aiomqtt
import pytest

from tests.broker import (
    HOST,
    MosquittoBroker,
    read_retained,
    silent_server,
    wait_or_dump,
    wait_until,
)
from tests.conftest import ConnectClient
from wb_mqtt_lib import (
    Client,
    ClientClosedError,
    ControlType,
    LoginRejectedError,
    Message,
)


@pytest.mark.parametrize("transport", ["unix", "ws", "mqtt-tcp"])
async def test_client_works_over_transport(
    broker: MosquittoBroker, connect: ConnectClient, transport: str
) -> None:
    """
    The client publishes and receives over the transport, a command makes the round trip via the broker.

    mqtt-tcp:// is tcp under the name wb-common takes, services bring such URLs from their configs.
    """
    client = await connect(transport=transport)
    device = await client.create_device("test_dev")
    relay = await device.create_control("relay", ControlType.SWITCH, value=False)
    commands = []
    relay.on_command(lambda control, value: commands.append(value))

    await client.raw.publish("/devices/test_dev/controls/relay/on", "1")

    await wait_until(lambda: commands == [True])
    retained = await read_retained(broker.port, "/devices/test_dev/controls/relay")
    assert retained == {"/devices/test_dev/controls/relay": b"0"}


@pytest.mark.parametrize(
    "path, request_line",
    [("/mqtt-ws", b"GET /mqtt-ws HTTP/1.1\r\n"), ("", b"GET /mqtt HTTP/1.1\r\n")],
    ids=["path in the URL", "no path"],
)
async def test_websocket_path_from_the_url_goes_into_the_handshake(path: str, request_line: bytes) -> None:
    """
    A broker behind a proxy may listen on another path. The path of a ws:// URL goes into the handshake,
    without one the client asks for /mqtt as paho does.
    """
    requests: list[bytes] = []

    async def record_request(reader: asyncio.StreamReader) -> None:
        requests.append(await reader.readline())

    async with silent_server(record_request) as port:
        client = Client("test-service", f"ws://{HOST}:{port}{path}")
        attempt = asyncio.ensure_future(client.connect())
        await wait_until(lambda: bool(requests))
        await wait_or_dump(client.close(), timeout=10)
        with pytest.raises(ClientClosedError):
            await wait_or_dump(attempt, timeout=5)

    assert requests[0] == request_line


async def test_subscriptions_go_with_qos_0_and_publications_with_qos_1(
    broker: MosquittoBroker, client: Client, driver: Client
) -> None:
    """
    The client subscribes with QoS 0 as wb-rules does, by the convention and raw by default. Only the
    markers that end retained delivery come with QoS 1. Publications keep QoS 1.
    """
    remote = await driver.create_device("remote")
    await remote.create_control("level", ControlType.VALUE, value=1)
    device = await client.create_device("test_dev")
    relay = await device.create_control("relay", ControlType.SWITCH, value=False)
    relay.on_command(lambda control, value: None)
    await client.get_control("remote/level")
    await client.get_device_names()
    raw_messages: list[Message] = []
    await client.raw.subscribe("/raw/#", raw_messages.append)
    published: list[Message] = []
    await driver.raw.subscribe("/devices/test_dev/controls/relay", published.append, qos=1)
    await driver.raw.publish("/raw/x", "1", qos=1)
    await wait_until(lambda: bool(raw_messages and published))

    subscriptions = broker.subscriptions()

    def granted(topic: str) -> set[int]:
        return {qos for topic_filter, qos in subscriptions if aiomqtt.Topic(topic).matches(topic_filter)}

    assert granted("/devices/test_dev/controls/relay/on") == {0}
    assert granted("/devices/remote/controls/level") == {0}
    assert granted("/raw/x") == {0}
    assert {qos for topic_filter, qos in subscriptions if topic_filter.startswith("/wbretainhack/")} == {1}
    assert raw_messages[0].qos == 0
    assert published[0].qos == 1


@pytest.fixture
def password() -> str:
    """
    Password of the user «service» at login_broker, a test parametrizes it to override.
    """
    return "secret"


@pytest.fixture
def login_broker(tmp_path: Path, password: str) -> Iterator[MosquittoBroker]:
    """
    Broker that accepts only the user «service» with the password.
    """
    with MosquittoBroker(tmp_path, users={"service": password}) as instance:
        yield instance


@pytest.mark.parametrize(
    "transport, password",
    [("tcp", "secret"), ("unix", "secret"), ("tcp", "p/s?s#w@r:d%"), ("tcp", "line\nbreak\x7f")],
    ids=["tcp", "unix", "percent-encoded", "control characters"],
)
async def test_valid_login_connects(login_broker: MosquittoBroker, transport: str, password: str) -> None:
    """
    User name and password from the URL reach the broker. A password with /, ?, #, @, : and % is written
    percent-encoded in the URL and reaches the broker decoded. A password is binary data in MQTT, so control
    characters in it are no reason to refuse the URL.
    """
    client = Client("test-service", login_broker.url(transport, f"service:{quote(password, safe='')}"))

    await wait_or_dump(client.connect(), timeout=5)

    assert client.connected
    await wait_or_dump(client.close(), timeout=5)


@pytest.mark.parametrize("login", ["service:wrong", ""], ids=["wrong password", "anonymous"])
async def test_rejected_login_raises_instead_of_reconnecting(
    login_broker: MosquittoBroker, login: str
) -> None:
    """
    Retrying a rejected login would not help, connect raises at once.
    """
    client = Client("test-service", login_broker.url("tcp", login))

    with pytest.raises(LoginRejectedError):
        await wait_or_dump(client.connect(), timeout=5)

    assert not client.connected
    await wait_or_dump(client.close(), timeout=5)


async def test_login_rejected_after_a_reconnect_is_retried(login_broker: MosquittoBroker) -> None:
    """
    The broker restarts and rejects the login for a while, the client keeps reconnecting.

    Once the broker takes the login again, the client is back with its devices.
    """
    url = login_broker.url("tcp", "service:secret")
    client = Client("test-service", url)
    reader = Client("reader", url)
    await wait_or_dump(client.connect(), timeout=5)
    try:
        await (await client.create_device("dev")).create_control("temp", ControlType.VALUE, value=21)

        login_broker.set_password("service", "changed")
        await asyncio.to_thread(login_broker.restart)
        await wait_until(lambda: not client.connected)
        await asyncio.sleep(2)
        assert not client.connected
        login_broker.set_password("service", "secret")
        await asyncio.to_thread(login_broker.restart)

        await wait_until(lambda: client.connected, timeout=20)
        await wait_or_dump(reader.connect(), timeout=5)
        temp = await reader.get_control("dev/temp", timeout=5)
        await wait_until(lambda: temp.value == 21)
    finally:
        await wait_or_dump(reader.close(), timeout=5)
        await wait_or_dump(client.close(), timeout=5)


@pytest.mark.parametrize(
    "url, secrets",
    [
        ("mqtt://127.0.0.1:1883", []),
        ("unix://", []),
        ("unix://tmp/mosquitto.sock", []),
        ("unix:///tmp/mosquitto?.sock", []),
        ("unix:///tmp/mosquitto#.sock", []),
        ("tcp://:1883", []),
        ("tcp://127.0.0.1", []),
        ("tcp://svc:ab/cd@host:1883", ["ab/cd", "svc"]),
        ("admin:s3cret@localhost:1883", ["admin", "s3cret"]),
        ("ws://svc:Zm9v/YmFy+==@host:18883", ["Zm9v", "YmFy", "svc"]),
        ("tcp://svc%0Aname:pw@host:1883", ["svc", "name", "pw"]),
        ("tcp://svc%EF%B7%90:pw@host:1883", ["svc", "pw"]),
        ("tcp://svc:secret@host:1883?keepalive=5", ["svc", "secret"]),
        ("ws://svc:secret@host:18883/mqtt#frag", ["svc", "secret"]),
    ],
)
def test_invalid_broker_url_raises_without_quoting_it(url: str, secrets: list[str]) -> None:
    """
    The URL may carry the password, the error text must not show any part of it.

    A unix socket path with two slashes, ? or # would silently point to another socket, and ? or # would
    silently cut a tcp or ws URL. On a login with a control character or a non-character mosquitto drops
    the connection, and the client would retry forever.
    """
    with pytest.raises(ValueError) as error:
        Client("test-service", url)
    assert not [secret for secret in secrets if secret in str(error.value)]
