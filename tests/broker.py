"""
Test helpers around a private mosquitto broker: the broker, a proxy that plays a dead link, reads and waits
for retained messages, a hook into publications, objects by name without a read. Also the deadlock detector
for asyncio code and data the tests share.
"""

import asyncio
import contextlib
import functools
import getpass
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

import aiomqtt

from wb_mqtt_lib import ControlType, broker_connection

HOST = "127.0.0.1"
MOSQUITTO = shutil.which("mosquitto") or "/usr/sbin/mosquitto"
MOSQUITTO_PASSWD = shutil.which("mosquitto_passwd") or "/usr/bin/mosquitto_passwd"
START_TIMEOUT_S = 5.0
STOP_TIMEOUT_S = 5.0
# A cancelled call may still wait for the broker, a connection attempt waits up to 5 s for CONNACK
CANCEL_TIMEOUT_S = 6.0
# JSON meta of the first control of type value without other fields, as the library publishes it
FIRST_VALUE_META = b'{"type": "value", "readonly": false, "hidden": false, "order": 1}'
# Device, control and driver names that would break the topic structure or that mosquitto drops
# the connection on: control characters, C1, a non-character
INVALID_NAMES = ["", "a/b", "a+b", "a#b", "a\0b", "a\nb", "a\x7fb", "a\x85b", "a\ufdd0b"]
# Every call of the client that reads the broker, for tests that must cover all of them
BROKER_READS: dict[str, Callable[[Any], Awaitable[Any]]] = {
    "create_device": lambda client: client.create_device("X"),
    "get_control": lambda client: client.get_control("dev/ctl"),
    "get_device": lambda client: client.get_device("dev"),
    "get_device_names": lambda client: client.get_device_names(),
    "get_device_snapshot": lambda client: client.get_device_snapshot("dev"),
    "remove_driver_devices": lambda client: client.remove_driver_devices(),
    "direct.read": lambda client: client.direct.read("/x/#"),
}


def internals(client: Any) -> dict[str, int]:
    """
    Counts inside a client that would grow on a leak.
    """
    # pylint: disable=protected-access
    return {
        "subscriptions": client._subscriber.subscription_count,
        "replay mirror": client._publisher.mirror_size,
        "listeners": len(client._listener_registry),
        "service tasks": len(client._service_tasks),
        "internal tasks": client._device_sync.task_count,
        "waits": client._device_sync.state.wait_count,
    }


def lose_end_markers(client: Any, monkeypatch: Any) -> None:
    """
    Lose the markers that end retained delivery, so a call that reads the broker keeps waiting.
    """
    # pylint: disable=protected-access
    monkeypatch.setattr(client._subscriber, "_marker_arrived", lambda topic: None)


def device_object(client: Any, name: str) -> Any:
    """
    Device object of the client by name without a read of the broker, the way the client builds it
    for a handler.
    """
    return client._make_device(name)  # pylint: disable=protected-access


def control_object(client: Any, full_name: str) -> Any:
    """
    Control object of the client by «device/control», see device_object.
    """
    device_name, _, control_name = full_name.partition("/")
    return client._make_control(device_name, control_name)  # pylint: disable=protected-access


# Hands the packet to paho and waits for PUBACK, as aiomqtt.Client.publish does
Send = Callable[[], Awaitable[None]]


def intercept_publish(monkeypatch: Any, intercept: Callable[[Send, str, Any], Awaitable[None]]) -> None:
    """
    Route every publication of every client in the test through intercept(send, topic, payload).

    Without a call of send the packet never leaves the client.
    """
    publish = aiomqtt.Client.publish

    async def intercepted(self: aiomqtt.Client, topic: str, *args: Any, **kwargs: Any) -> None:
        await intercept(
            functools.partial(publish, self, topic, *args, **kwargs),
            topic,
            args[0] if args else kwargs.get("payload"),
        )

    monkeypatch.setattr(aiomqtt.Client, "publish", intercepted)


def ack_timeout() -> aiomqtt.MqttError:
    """
    What aiomqtt raises when the broker does not acknowledge in time.
    """
    return aiomqtt.MqttError("Operation timed out")


class DeadlockSuspectedError(AssertionError):
    """
    An awaited result did not arrive in time. The message lists what every asyncio task was awaiting.
    """


def get_free_ports(count: int) -> list[int]:
    """
    Return loopback TCP ports that are free right now and differ from each other.

    The sockets stay open until all ports are taken, otherwise the system may hand out one port twice.
    """
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for sock in sockets:
            sock.bind((HOST, 0))
        return [sock.getsockname()[1] for sock in sockets]


class MosquittoBroker:
    """
    Private mosquitto without persistence, like the broker on the controller.

    It listens on tcp and websockets loopback ports and on a unix socket, the three transports a service
    can use. With users it accepts only those logins. As a context manager it starts and cleans up.
    """

    def __init__(self, workdir: Path, users: Optional[dict[str, str]] = None) -> None:
        self.port, self.ws_port = get_free_ports(2)
        # A unix socket path is limited to 108 bytes, pytest directories of long test names do not fit
        self._socket_dir = Path(tempfile.mkdtemp(prefix="wbmq-"))
        self.socket_path = self._socket_dir / "mosquitto.sock"
        self._config = workdir / "mosquitto.conf"
        self._log = workdir / "mosquitto.log"
        lines = [
            # Started by root, mosquitto would switch to its own user and lose access to the test directory
            f"user {getpass.getuser()}",
            f"listener {self.port} {HOST}",
            f"listener {self.ws_port} {HOST}",
            "protocol websockets",
            f"listener 0 {self.socket_path}",
            "persistence false",
            # Limits of the broker on the controller, /usr/share/wb-configs/mosquitto/30limits.conf
            "max_queued_messages 0",
            "max_inflight_messages 1000",
            "memory_limit 100000000",
            # Default types plus subscriptions and unsubscriptions, for tests of what a client subscribes to
            "log_type error",
            "log_type warning",
            "log_type notice",
            "log_type information",
            "log_type subscribe",
            "log_type unsubscribe",
        ]
        if users:
            for index, (username, password) in enumerate(users.items()):
                create = ["-c"] if index == 0 else []
                subprocess.run(
                    [MOSQUITTO_PASSWD, *create, "-b", str(self._password_file), username, password],
                    check=True,
                )
            lines += ["allow_anonymous false", f"password_file {self._password_file}"]
        else:
            lines.append("allow_anonymous true")
        self._config.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self._process: Optional[subprocess.Popen] = None

    def __enter__(self) -> "MosquittoBroker":
        try:
            self.start()
        except BaseException:
            self.cleanup()
            raise
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.cleanup()

    @property
    def _password_file(self) -> Path:
        return self._config.with_name("passwd")

    def url(self, transport: str = "tcp", login: str = "") -> str:
        """
        Broker URL for the library. login is «user:password».
        """
        credentials = f"{login}@" if login else ""
        if transport == "unix":
            return f"unix://{credentials}{self.socket_path}"
        return f"{transport}://{credentials}{HOST}:{self.ws_port if transport == 'ws' else self.port}"

    def subscriptions(self) -> list[tuple[str, int]]:
        """
        (filter, QoS) of every subscription the broker took, in order, from its log.
        """
        found = []
        for line in self._log.read_text(errors="replace").splitlines():
            # «<time>: <client id> <QoS> <filter>»
            parts = line.split(" ", 3)
            if len(parts) == 4 and parts[2] in ("0", "1", "2"):
                found.append((parts[3], int(parts[2])))
        return found

    def unsubscriptions(self) -> list[str]:
        """
        Filter of every unsubscription the broker took, in order, from its log.
        """
        found = []
        for line in self._log.read_text(errors="replace").splitlines():
            # «<time>: <client id> <filter>», the filters of the tests have no spaces
            parts = line.split(" ")
            if len(parts) == 3:
                found.append(parts[2])
        return found

    def start(self) -> None:
        """
        Start mosquitto and return once all three listeners accept connections.
        """
        with self._log.open("ab") as log:
            self._process = subprocess.Popen(  # pylint: disable=consider-using-with
                [MOSQUITTO, "-c", str(self._config)], stdout=log, stderr=log
            )
        try:
            self._wait_listening()
        except BaseException:
            # A broker that did not start would outlive the test run
            self.stop()
            raise

    def stop(self) -> None:
        """
        Stop mosquitto, kill it when it does not stop in time. The broker keeps nothing between runs.
        """
        if self._process is None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=STOP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()
        self._process = None

    def restart(self) -> None:
        """
        Restart mosquitto on the same ports.
        """
        self.stop()
        self.start()

    def set_password(self, username: str, password: str) -> None:
        """
        Change the password of a user of a broker created with users. The broker reads it at the next start.
        """
        subprocess.run([MOSQUITTO_PASSWD, "-b", str(self._password_file), username, password], check=True)

    def cleanup(self) -> None:
        """
        Stop mosquitto and remove the socket directory.
        """
        try:
            self.stop()
        finally:
            shutil.rmtree(self._socket_dir, ignore_errors=True)

    def _wait_listening(self) -> None:
        deadline = time.monotonic() + START_TIMEOUT_S
        for family, address in [
            (socket.AF_INET, (HOST, self.port)),
            (socket.AF_INET, (HOST, self.ws_port)),
            (socket.AF_UNIX, str(self.socket_path)),
        ]:
            while True:
                try:
                    with socket.socket(family) as sock:
                        sock.settimeout(0.2)
                        sock.connect(address)
                    break
                except OSError as error:
                    if self._process.poll() is not None:
                        raise RuntimeError(
                            f"mosquitto exited: {self._log.read_text(errors='replace')}"
                        ) from error
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"mosquitto does not listen on {address}") from error
                    time.sleep(0.02)


def describe_tasks() -> str:
    """
    Describe every asyncio task as the chain of coroutines it is awaiting, innermost last.

    Task.get_stack() shows only the outer frame of a suspended coroutine, so the chain is walked via cr_await.
    """
    lines = []
    for task in asyncio.all_tasks():
        chain = []
        awaited: Any = task.get_coro()
        while awaited is not None:
            frame = getattr(awaited, "cr_frame", None)
            if frame is None:
                chain.append(type(awaited).__name__)
                break
            chain.append(f"{awaited.__qualname__}:{frame.f_lineno}")
            awaited = getattr(awaited, "cr_await", None)
        lines.append(f"{task.get_name()}: {' -> '.join(chain)}")
    return "\n".join(lines)


async def wait_or_dump(awaitable: Awaitable, timeout: float) -> Any:
    """
    Await a result. On timeout, describe all tasks before cancelling and raise DeadlockSuspectedError.
    """
    future = asyncio.ensure_future(awaitable)
    done, _ = await asyncio.wait({future}, timeout=timeout)
    if future in done:
        return future.result()
    tasks = describe_tasks()
    future.cancel()
    # Let the cancelled call clean up before the test ends and its event loop closes
    await asyncio.wait({future}, timeout=CANCEL_TIMEOUT_S)
    raise DeadlockSuspectedError(f"no result in {timeout} s, asyncio tasks:\n{tasks}")


async def wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    """
    Wait for a condition that has no event to await, checking it every 20 ms.
    """

    async def poll() -> None:
        while not condition():
            await asyncio.sleep(0.02)

    await wait_or_dump(poll(), timeout)


async def wait_for_task_count(count: int, timeout: float = 5.0) -> None:
    """
    Wait until the event loop runs count tasks. wait_until would add a task of its own to the count.
    """
    for _ in range(int(timeout / 0.02)):
        if len(asyncio.all_tasks()) == count:
            return
        await asyncio.sleep(0.02)
    assert len(asyncio.all_tasks()) == count


@contextlib.asynccontextmanager
async def silent_server(serve: Callable[[asyncio.StreamReader], Awaitable[Any]]) -> AsyncIterator[int]:
    """
    TCP server on a loopback port that takes connections and never answers. serve(reader) reads what
    a connection sends, then the connection closes. Yields the port.
    """
    connections: list[asyncio.StreamWriter] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connections.append(writer)
        await serve(reader)
        writer.close()

    server = await asyncio.start_server(handle, HOST, 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        # By hand, the server of uvloop has no close_clients
        for writer in connections:
            writer.close()
        await server.wait_closed()


async def cancel_and_wait(task: asyncio.Future) -> None:
    """
    Cancel a task and wait until it ends. A task that ended with another error raises it.
    """
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def restart_broker(broker: MosquittoBroker, *clients: Any) -> None:
    """
    Restart the broker and wait until the clients are connected again.
    """
    await asyncio.to_thread(broker.restart)
    await wait_until(lambda: all(client.is_connected for client in clients), timeout=10)


async def wait_retained(
    port: int, topic_filter: str, expected: dict[str, bytes], timeout: float = 10.0
) -> dict[str, bytes]:
    """
    Wait until the retained messages under a filter are exactly the expected ones.

    The client publishes its own state again in a task of its own after reconnect, so the broker gets it a
    moment after the client reports the link.
    """
    retained: dict[str, bytes] = {}

    async def poll() -> None:
        nonlocal retained
        while True:
            retained = await read_retained(port, topic_filter)
            if retained == expected:
                return
            await asyncio.sleep(0.05)

    try:
        await wait_or_dump(poll(), timeout)
    except DeadlockSuspectedError as error:
        raise AssertionError(f"retained under {topic_filter} is {retained}, expected {expected}") from error
    return retained


async def window_of_controls(client: Any) -> list[Any]:
    """
    Own controls c0, c1 and so on of the device dev, as many as the client keeps waiting for PUBACK at once,
    of type value with the value 0.
    """
    device = await client.create_device("dev")
    return [
        await device.create_control(f"c{index}", ControlType.VALUE, value=0)
        for index in range(broker_connection.SEND_WINDOW)
    ]


async def publish_retained(client: Any, retained: dict[str, str]) -> None:
    """
    Publish retained messages through a client in the order given, as a driver leaves them in the broker.
    """
    for topic, payload in retained.items():
        await client.direct.publish(topic, payload, retain=True)


async def read_retained(port: int, topic_filter: str, timeout: float = 5.0) -> dict[str, bytes]:
    """
    Read retained messages under a filter. The end of delivery is a unique marker topic.
    """
    marker = f"/test-marker/{uuid.uuid4().hex}"
    retained = {}
    async with aiomqtt.Client(HOST, port) as client:
        await client.subscribe(topic_filter)
        await client.subscribe(marker)
        await client.publish(marker, "1")

        async def collect() -> None:
            async for message in client.messages:
                if message.topic.matches(marker):
                    return
                retained[str(message.topic)] = message.payload

        await wait_or_dump(collect(), timeout)
    return retained


class DeadLinkProxy:
    """
    TCP proxy in front of the broker that plays a link that is dead but not closed yet.

    While dead is True, everything both ways is read and thrown away. cut() closes every connection, which
    is how such a link finally ends.
    """

    def __init__(self, broker_port: int) -> None:
        self.broker_port = broker_port
        self.port = 0
        self.dead = False
        self._server: Optional[asyncio.AbstractServer] = None
        self._writers: set[asyncio.StreamWriter] = set()

    def url(self) -> str:
        return f"tcp://{HOST}:{self.port}"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, HOST, 0)
        self.port = self._server.sockets[0].getsockname()[1]

    def cut(self) -> None:
        for writer in list(self._writers):
            writer.transport.abort()

    async def drop(self, client: Any) -> None:
        """
        Close the link of the client and keep it dead, the client cannot connect again until restore.
        """
        self.dead = True
        self.cut()
        await wait_until(lambda: not client.is_connected)

    async def restore(self, client: Any) -> None:
        """
        Bring the link back and wait until the client is connected again.
        """
        self.dead = False
        await wait_until(lambda: client.is_connected, timeout=20)

    async def close(self) -> None:
        self.cut()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _serve(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        try:
            broker_reader, broker_writer = await asyncio.open_connection(HOST, self.broker_port)
        except OSError:
            client_writer.transport.abort()
            return
        writers = {client_writer, broker_writer}
        self._writers |= writers
        try:
            await asyncio.gather(
                self._pipe(client_reader, broker_writer), self._pipe(broker_reader, client_writer)
            )
        finally:
            self._writers -= writers
            for writer in writers:
                writer.transport.abort()

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    return
                if not self.dead:
                    writer.write(data)
                    await writer.drain()
        except (ConnectionError, OSError):
            return
        finally:
            writer.transport.abort()
