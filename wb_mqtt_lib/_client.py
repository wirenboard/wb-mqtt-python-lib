"""
Client: one broker connection, reconnect, the devices of the service and its background tasks.
"""

import asyncio
import contextlib
import uuid
from typing import Any, Coroutine, Optional

from ._convention.device_manager import DeviceManager
from ._convention.devices import Control, Device
from ._convention.state import State
from ._convention.values import DeviceSnapshot, TitleLike, check_name
from ._errors import ClientClosedError, InvalidNameError, LoginRejectedError
from ._listeners import (
    ConnectionHandler,
    Handler,
    ListenerRegistry,
    ListenerSet,
    check_handler_arguments,
)
from ._logs import get_logger
from ._mqtt import broker_connection
from ._mqtt.broker_url import DEFAULT_BROKER_URL, parse_broker_url
from ._mqtt.publisher import Publisher
from ._mqtt.raw_topics import RawTopics
from ._mqtt.subscriber import Subscriber
from ._tasks import TaskSet

logger = get_logger(__name__)

RECONNECT_MIN_DELAY_S = 0.5
RECONNECT_MAX_DELAY_S = 30.0
# Drivers of the clients connected in this process. Cleanup goes by the driver, so it must not be shared
_DRIVERS_IN_USE: set[str] = set()


class Client:  # pylint: disable=too-many-instance-attributes
    """
    One connection to the broker and the devices of the service, for the whole time the service runs.

    A service usually needs one client. A second one makes sense only for devices under another driver name,
    each client has its own devices and sees those of the other as foreign. A closed client stays closed.

    Examples:
        async with Client(driver="wb-mqtt-example") as client:
            ...

    Args:
        driver: driver name of the service. It goes into the meta of every own device and into the client id.
        broker_url: unix:///path/to/socket, tcp://host:port or ws://host:port/path. A login goes
            as user:password@ before the host, percent-encoded.

    Raises:
        InvalidNameError: the driver name is invalid.
        ValueError: the URL is invalid.
    """

    def __init__(self, driver: str, broker_url: str = DEFAULT_BROKER_URL) -> None:
        check_name(driver, "driver")
        self.driver = driver
        self._address = parse_broker_url(broker_url)

        # Components the client hands the work to
        self._subscriber = Subscriber()
        self._publisher = Publisher()
        self._listener_registry = ListenerRegistry()
        self._connection_listeners = ListenerSet(self._listener_registry, f"client {driver}")
        state = State(self._listener_registry, self._make_control, self._make_device)
        self._device_manager = DeviceManager(
            driver, self._subscriber, self._publisher, state, is_closed=self._is_closed
        )
        self._service_tasks = TaskSet(logger)
        self.raw = RawTopics(
            self._subscriber, self._publisher, self._listener_registry, is_closed=self._is_closed
        )

        # Sessions with the broker, from connect() to close()
        self._client_id = ""
        self._session_task: Optional[asyncio.Task] = None
        self._connected_event = asyncio.Event()
        self._stop_requested = asyncio.Event()
        self._fatal_error: Optional[BaseException] = None
        self._holds_driver = False

        # Closing
        self._closed = False
        self._close_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        await self.close()

    @property
    def is_connected(self) -> bool:
        """
        True while the broker link is up.

        It may drop right after the check, so a command is not guarded by it. A command without a link raises
        BrokerDisconnectedError, catch that instead.
        """
        return self._subscriber.is_connected

    async def connect(self) -> None:
        """
        Connect and return after the first successful connection. Retries until the broker answers.

        If the call fails or is cancelled, the client stops connecting and frees the driver name at once.
        An attempt that has begun ends by itself, by the broker answer or in 5 s, and the next connect()
        or close() waits for that.

        Raises:
            RuntimeError: the client is connected already. It reconnects by itself after a lost link.
            ValueError: another client in this process is connected with the same driver.
            LoginRejectedError: the broker rejected the login. Later, after a reconnect, a rejected login
                is retried like a lost link.
            ClientClosedError: the client is closed or closes while connecting.
        """
        self._raise_if_closed()
        if self._holds_driver:
            raise RuntimeError("the client is connected already, it reconnects by itself after a lost link")
        if self.driver in _DRIVERS_IN_USE:
            raise ValueError(f"driver {self.driver} is taken by another client in this process")

        _DRIVERS_IN_USE.add(self.driver)
        self._holds_driver = True

        try:
            previous = self._session_task
            if previous is not None and not previous.done():
                # The sessions of a cancelled call share the stop flag, the subscriptions and the queue
                await asyncio.shield(previous)
                self._raise_if_closed()

            self._client_id = f"{self.driver}-{uuid.uuid4().hex[:8]}"
            self._stop_requested.clear()
            self._fatal_error = None
            self._session_task = asyncio.create_task(
                self._run_sessions(), name=f"mqtt sessions {self.driver}"
            )
            connected = asyncio.create_task(self._connected_event.wait())

            try:
                await asyncio.wait({connected, self._session_task}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                connected.cancel()

            if self._fatal_error is not None:
                raise self._fatal_error
            if self._closed:
                raise ClientClosedError("the client is closed")
        except BaseException:
            # Without a wait here another cancellation of the caller cannot reach the sessions
            self._request_stop()
            raise

    async def close(self) -> None:
        """
        Stop handlers and background tasks, then disconnect. Devices stay in the broker.

        Other code that waits for the broker gets ClientClosedError. Own changes not sent while the link
        was down are dropped. Safe to call from a handler or a background task of this client and from
        several tasks at once. A cancelled call stops waiting, the cleanup goes on.
        """
        caller = asyncio.current_task()
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close(caller), name=f"close {self.driver}")
        elif caller in self._service_tasks or self._listener_registry.is_listener_task(caller):
            return
        await asyncio.shield(self._close_task)

    def on_connection_change(self, handler: ConnectionHandler) -> ConnectionHandler:
        """
        handler(connected) on every change of the broker link after the registration.

        The first connection is a change too, so a handler registered before connect() gets True when
        the client connects. The handler is not called at the registration, the current state is in
        connected. True comes after the client subscribed again. A lost link and the failed attempts after it
        give one False, before the first connection there is none, and close() gives none.

        Raises:
            TypeError: the handler does not take one argument.
            ClientClosedError: the client is closed.
        """
        self._raise_if_closed()
        check_handler_arguments(handler, 1, "on_connection_change")
        return self._connection_listeners.add_handler("connection", handler)

    def remove_handler(self, handler: Handler) -> None:
        """
        Take a handler off on_connection_change.

        Events the handler has not started are dropped. A call that runs now is cancelled at its next await,
        unless remove_handler is called from that handler itself.
        """
        self._connection_listeners.remove_handler(handler)

    # ------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------

    def start_task(self, coroutine: Coroutine[Any, Any, Any], name: Optional[str] = None) -> asyncio.Task:
        """
        Run work in the background. The client keeps the task, logs its error and cancels it on close.

        Raises:
            ClientClosedError: the client is closed.
        """
        if self._closed:
            coroutine.close()
            raise ClientClosedError("the client is closed")
        return self._service_tasks.start_task(coroutine, name or getattr(coroutine, "__qualname__", "task"))

    # ------------------------------------------------------------
    # Devices
    # ------------------------------------------------------------

    async def create_device(self, name: str, title: TitleLike = None, force: bool = False) -> Device:
        """
        Publish an own device. Its meta carries the driver name of the client.

        First the client reads what the broker keeps under the name. Leftovers of the same driver, for example
        of a previous run, are erased, so the device starts clean. The driver comes from the JSON meta of
        the device, or from the legacy /meta/driver when the JSON meta names none. A device of another driver
        or a device without a driver stays untouched. Topics that make no device, values without meta
        for example, are leftovers and are erased.

        Args:
            name: device name, not empty and without /, +, # and control characters.
            title: a string or a {"en": ..., "ru": ...} mapping.
            force: erase whatever the broker keeps under the name, a device of another driver too, and publish
                the own device. Another driver that still runs keeps publishing its values there and
                publishes its meta again after its reconnect.

        Raises:
            InvalidNameError: the name is invalid.
            AlreadyCreatedError: this client created the device already.
            TypeError: the title is not a string or a {"en": ..., "ru": ...} mapping.
            DeviceTakenError: the name belongs to a device of another driver or a device without a driver,
                and force is False.
            BrokerDisconnectedError: no broker link to check the name.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_manager.create_device(name, title, force)
        return self._make_device(name)

    async def get_device(self, name: str, timeout: Optional[float] = None) -> Device:
        """
        Device by name.

        Without timeout it waits only for the broker answer and returns the object even when the device
        does not exist. exists is then False and on_added fires when it appears.

        Raises:
            InvalidNameError: the name has forbidden characters.
            TimeoutError: timeout is given and the device did not appear in time.
            BrokerDisconnectedError: no broker link to read a device not read before.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_manager.watch_device(name)
        if timeout is not None:
            await self._device_manager.wait_device_exists(name, timeout)
        return self._make_device(name)

    async def get_control(self, address: str, timeout: Optional[float] = None) -> Control:
        """
        Control by address «device/control». Without timeout it waits only for the broker answer,
        like get_device.

        Raises:
            InvalidNameError: the address is not «device/control» or has forbidden characters.
            TimeoutError: timeout is given and the control did not appear in time.
            BrokerDisconnectedError: no broker link to read a device not read before.
            ClientClosedError: the client is closed or closes while waiting.
        """
        device_name, separator, control_name = address.partition("/")
        if not separator:
            raise InvalidNameError(f"control address {address!r} must look like device/control")
        check_name(control_name, "control")
        await self._device_manager.watch_device(device_name)
        if timeout is not None:
            await self._device_manager.wait_control_exists(device_name, control_name, timeout)
        return self._make_control(device_name, control_name)

    async def get_device_names(self) -> list[str]:
        """
        Names of the devices in the broker now, one-off read of device and control meta.

        Raises:
            BrokerDisconnectedError: no broker link.
            ClientClosedError: the client is closed or closes while waiting.
        """
        return await self._device_manager.get_device_names()

    async def get_device_snapshot(self, name: str) -> Optional[DeviceSnapshot]:
        """
        Device as the broker keeps it now, one-off read without a lasting subscription.

        The snapshot does not change afterwards, a device that follows the broker comes from get_device.
        Values come typed by the meta.

        Returns:
            The snapshot, None when the broker has no device under the name.

        Raises:
            InvalidNameError: the name is invalid.
            BrokerDisconnectedError: no broker link.
            ClientClosedError: the client is closed or closes while waiting.
        """
        return await self._device_manager.get_device_snapshot(name)

    async def remove_driver_devices(self) -> list[str]:
        """
        Erase every device in the broker whose meta names the driver of this client.

        Returns:
            Names of the erased devices.

        Raises:
            BrokerDisconnectedError: no broker link.
            ClientClosedError: the client is closed or closes while waiting.
        """
        return await self._device_manager.remove_driver_devices()

    # ------------------------------------------------------------
    # Sessions with the broker
    # ------------------------------------------------------------

    async def _run_sessions(self) -> None:
        delay = RECONNECT_MIN_DELAY_S
        failure_logged = False
        # A rejected login is fatal only before the first connection. Later the broker has taken this login
        # once, the retries connect when it takes it again, a stopped client the service would rebuild
        # with its devices
        connected_once = False
        while not self._stop_requested.is_set():
            reported_connected = False
            try:
                async with broker_connection.Connection(self._address, self._client_id) as connection:
                    connected_once = True
                    await self._subscriber.connection_restored(connection)
                    self._publisher.connection_restored(connection)
                    await self._subscriber.publish_resubscription_marker()
                    self._connected_event.set()
                    self._connection_listeners.emit_event("connection", (True,))
                    reported_connected = True
                    # A session that keeps failing before this point keeps the pause growing
                    delay = RECONNECT_MIN_DELAY_S
                    failure_logged = False
                    logger.info("Connected to the MQTT broker")
                    await self._read_until_stop(connection)
                    return
            except broker_connection.LinkError as error:
                if isinstance(error, broker_connection.LoginRejected) and not connected_once:
                    self._fatal_error = LoginRejectedError(str(error))
                    logger.error("MQTT login rejected: %s", error)
                    return
                if self._stop_requested.is_set():
                    # close() is under way and no reconnect follows. On a dead link DISCONNECT gets no answer.
                    logger.debug("MQTT connection closed: %s", error)
                elif not failure_logged:
                    failure_logged = True
                    # Normal while the broker restarts, the client reconnects by itself
                    if connected_once:
                        logger.warning("MQTT broker connection lost: %s. Reconnecting", error)
                    else:
                        logger.warning("MQTT broker is not available: %s. Retrying", error)
            except Exception:  # pylint: disable=broad-exception-caught
                # A bug must not leave the service without a broker for good
                logger.exception("MQTT session failed. Reconnecting")
            finally:
                self._connected_event.clear()
                self._subscriber.connection_lost()
                self._publisher.connection_lost()
                if reported_connected:
                    self._connection_listeners.emit_event("connection", (False,))
                await self._publisher.wait_ended_session()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_requested.wait(), delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_S)

    async def _read_until_stop(self, connection: broker_connection.Connection) -> None:
        """
        Hand incoming messages to the subscriber. Return on close, raise LinkLost when the link drops.
        """
        reader = asyncio.create_task(
            connection.read_messages(self._subscriber.dispatch), name="read messages"
        )
        closing = asyncio.create_task(self._stop_requested.wait(), name="wait close")
        try:
            done, _ = await asyncio.wait({reader, closing}, return_when=asyncio.FIRST_COMPLETED)
            if reader in done:
                # read ends only with an error
                reader.result()
        finally:
            for task in (reader, closing):
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, broker_connection.LinkError):
                    await task

    async def _close(self, caller: Optional[asyncio.Task]) -> None:
        # Handlers and tasks of the client end by cancellation, not by an error of the wait they are in.
        # A loop that catches Exception around a wait would otherwise spin and never let close go on.
        self._subscriber.mark_closing()
        self._publisher.mark_closing()
        await self._listener_registry.stop_all(spare=caller)
        await self._service_tasks.cancel_all_tasks(spare=caller)
        self._subscriber.fail_waits()
        await self._device_manager.close()
        await self._stop_sessions()

    def _request_stop(self) -> None:
        """
        Free the driver name at once and tell the sessions to stop. A connection attempt that has begun ends
        only by the broker answer or its timeout.
        """
        self._release_driver()
        self._stop_requested.set()

    async def _stop_sessions(self) -> None:
        self._request_stop()
        if self._session_task is not None:
            await self._session_task

    def _release_driver(self) -> None:
        if self._holds_driver:
            _DRIVERS_IN_USE.discard(self.driver)
            self._holds_driver = False

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise ClientClosedError("the client is closed")

    def _is_closed(self) -> bool:
        return self._closed

    def _make_control(self, device_name: str, control_name: str) -> Control:
        return Control(device_name, control_name, self._device_manager)

    def _make_device(self, device_name: str) -> Device:
        return Device(device_name, self._device_manager)
