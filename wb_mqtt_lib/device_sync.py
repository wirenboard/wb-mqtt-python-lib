"""
Device sync of a client. Foreign devices are read from the broker, own devices are published and take
commands, the state lives in state.py.

An operation on an own device changes the state and the publisher mirror without an await in between
and only then waits for the broker, so a cancelled call never leaves a change half sent.
"""

import asyncio
import dataclasses
import logging
from typing import Any, Callable, Iterable, Optional

from . import topics
from .errors import (
    BrokerDisconnectedError,
    ClientClosedError,
    ControlMissingError,
    DeviceTakenError,
    NotOwnError,
)
from .listeners import Handler, ListenerRegistry
from .publisher import Publisher
from .state import ControlRecord, DeviceRecord, Ownership, State
from .subscriber import Message, Subscriber
from .tasks import TaskSet, cancelled_from_outside, retrieve_error
from .values import (
    ControlErrorFlags,
    ControlMeta,
    ControlSnapshot,
    ControlType,
    DeviceMeta,
    DeviceSnapshot,
    TitleLike,
    TranslatedTitle,
    check_name,
    parse_control_meta,
    parse_device_meta,
    parse_meta_fields,
    parse_value,
    to_payload_and_value,
)

logger = logging.getLogger(__name__)

# Name prefix of the tasks that finish create_device
_CREATION_TASK_PREFIX = "create_"


class DeviceSync:  # pylint: disable=too-many-public-methods
    """
    Devices of one client: subscriptions, publication and the internal tasks around them.

    The client builds it with its own subscriber, publisher and state. is_closed tells whether close()
    of the client was called.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        driver: str,
        subscriber: Subscriber,
        publisher: Publisher,
        state: State,
        *,
        is_closed: Callable[[], bool],
    ) -> None:
        self._driver = driver
        self._subscriber = subscriber
        self._publisher = publisher
        self.state = state
        self._is_closed = is_closed
        # Subscription to the whole of a foreign device, kept until the client closes
        self._device_watches: dict[str, asyncio.Task] = {}
        self._tasks = TaskSet(logger)

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    async def close(self) -> None:
        """
        End the waits with ClientClosedError and stop subscriptions and erasures in progress.
        """
        self.state.fail_waits(ClientClosedError("the client is closed"))
        await self._tasks.cancel_all_tasks(self._device_watches.values())

    # ------------------------------------------------------------
    # Reading devices, own and foreign
    # ------------------------------------------------------------

    async def watch_device(self, name: str) -> None:
        """
        Subscribe to the whole device unless it is own, return after the broker delivered what it keeps.

        Raises:
            ValueError: the name is invalid.
            BrokerDisconnectedError: no broker link to read the device.
            ClientClosedError: the client is closed or closes while waiting.
        """
        self._raise_if_closed()
        check_name(name, "device")
        device = self.state.ensure_device(name)
        # An own device is in the state already, there is nothing to read
        if device.ownership is Ownership.OWN:
            return

        device_watch = self._device_watches.get(name)
        if device_watch is None:
            device_watch = asyncio.create_task(self._subscribe_device(name), name=f"subscribe {name}")
            device_watch.add_done_callback(lambda task: self._forget_failed_device_watch(name, task))
            self._device_watches[name] = device_watch
        await self._wait_for_broker(device_watch)

    def _forget_failed_device_watch(self, name: str, device_watch: asyncio.Task) -> None:
        # The next call subscribes again, also when every caller of this one was cancelled meanwhile
        failed = not device_watch.cancelled() and device_watch.exception() is not None
        if failed and self._device_watches.get(name) is device_watch:
            del self._device_watches[name]

    async def _subscribe_device(self, name: str) -> None:
        try:
            await self._subscriber.subscribe(topics.watch_filters(name), self._on_device_message)
        except BaseException:
            # Undo the subscription, the next call subscribes again
            for topic_filter in topics.watch_filters(name):
                await self._subscriber.unsubscribe(topic_filter, self._on_device_message)
            raise

    async def wait_device_exists(self, name: str, timeout: float) -> None:
        self._raise_if_closed()
        await self.state.wait_device_exists(name, timeout)

    async def wait_control_exists(self, device_name: str, control_name: str, timeout: float) -> None:
        self._raise_if_closed()
        await self.state.wait_control_exists(device_name, control_name, timeout)

    async def wait_control_value(
        self, device_name: str, control_name: str, expected: Any, timeout: float
    ) -> Any:
        self._raise_if_closed()
        self._check_device_watched(device_name)
        return await self.state.wait_control_value(device_name, control_name, expected, timeout)

    async def get_device_names(self) -> list[str]:
        self._raise_if_closed()
        retained = await self._subscriber.read_retained([topics.ALL_DEVICE_METAS, topics.ALL_CONTROL_METAS])
        return _build_state(retained).existing_device_names()

    async def get_device_snapshot(self, name: str) -> Optional[DeviceSnapshot]:
        self._raise_if_closed()
        check_name(name, "device")
        return _build_snapshot(name, await self._subscriber.read_retained([topics.device_tree(name)]))

    # ------------------------------------------------------------
    # Changes of own devices and commands to foreign controls
    # ------------------------------------------------------------

    async def create_device(self, name: str, title: TitleLike, force: bool = False) -> None:
        # Checks that need no broker
        self._raise_if_closed()
        check_name(name, "device")
        title = TranslatedTitle.parse(title)
        device_topics = topics.DeviceTopics(
            meta=topics.CheckedTopic(topics.device_meta(name)),
            error=topics.CheckedTopic(topics.device_error(name)),
        )
        device = self.state.ensure_device(name)
        _check_not_created(name, device)

        # What the broker keeps under the name
        leftovers = await self._subscriber.read_retained([topics.device_tree(name)])
        # Another create_device of this client may have finished meanwhile, its device is not a leftover
        _check_not_created(name, device)

        # Who owns the name
        signed, owner = _get_device_signature(name, leftovers)
        foreign = signed and owner != self._driver
        if foreign and not force:
            raise DeviceTakenError(f"device {name} belongs to driver {owner!r}")
        if foreign:
            logger.debug("Device %s of driver %r is replaced by force", name, owner)
        elif leftovers:
            logger.debug("Device %s is left from a previous run, erasing %d topics", name, len(leftovers))

        # A cancelled caller stops waiting, the device still becomes own
        created = self._tasks.start_task(
            self._take_over_name(name, title, device_topics, leftovers),
            f"{_CREATION_TASK_PREFIX}{name}",
            log_error=False,
        )
        await self._wait_for_broker(created)

    async def _take_over_name(
        self,
        name: str,
        title: TranslatedTitle,
        device_topics: topics.DeviceTopics,
        leftovers: dict[str, bytes],
    ) -> None:
        """
        Erase the leftovers under the name and publish the own device in their place.

        Once the name is checked, the device becomes own also without a link. Its erasures and meta wait
        in the mirror in their order and go out after reconnect, like any own change.
        """
        # Leftovers are erased before the own device takes the name
        if leftovers:
            await self._erase_leftovers(leftovers)

        # The name becomes own unless another create_device took it meanwhile
        device = self.state.ensure_device(name)
        _check_not_created(name, device)
        self.state.make_device_own(name, title, self._driver)
        device.topics = device_topics

        # The meta goes out, the commands get subscribed, waits for the device end
        published = self._publisher.queue_retained([(device.topics.meta, self._get_device_meta_json(device))])
        subscribed = self._tasks.start_task(self._subscribe_commands(name), f"subscribe commands of {name}")
        self.state.wake_device_waits(name)
        await asyncio.gather(published, subscribed)

    async def update_device_meta(self, name: str, title: TitleLike) -> None:
        device = self._require_own_device(name)
        title = TranslatedTitle.parse(title)
        if title == device.title:
            return
        self.state.set_device_meta(name, DeviceMeta(title, self._driver))
        # Waits for PUBACK, then a reader of the name nearly always gets the echo before a remove() that
        # follows. An echo of the meta after it brings the name back as a foreign device.
        await self._publisher.publish(
            device.topics.meta, self._get_device_meta_json(device), qos=1, retain=True
        )

    async def remove_device(self, name: str) -> None:
        # A second call does nothing
        self._raise_if_closed()
        if self._is_removed(name):
            return
        device = self._require_own_device(name)
        if any(control.listeners.has_listeners("command") for control in device.controls.values()) or (
            device.listeners.has_listeners("control_command")
        ):
            logger.debug("Device %s is removed, its command handlers wait until it is created again", name)

        # Topics to erase, the controls in reverse order, then the error and the meta of the device
        to_erase = [
            topic
            for control_name in reversed(self.state.existing_control_names(name))
            for topic in _get_control_topics_to_erase(device.controls[control_name])
        ]
        if device.error:
            to_erase.append(device.topics.error)
        to_erase.append(device.topics.meta)

        # Removed before the erasure, so its echoes apply like those of a foreign device
        self.state.mark_device_removed(name)
        erasure_done = self._erase(self._add_own_topics_under(to_erase, topics.device_root(name)))
        await self._wait_for_broker(
            self._tasks.start_task(self._unsubscribe_commands_after(name, erasure_done), f"remove {name}")
        )

    async def create_control(  # pylint: disable=too-many-arguments,too-many-locals
        self, device_name: str, control_name: str, control_type: str, value: Any = None, **meta_fields: Any
    ) -> None:
        # Checks of the name, the type and the device
        self._raise_if_closed()
        check_name(control_name, "control")
        control_topics = topics.ControlTopics(
            value=topics.CheckedTopic(topics.control_value(device_name, control_name)),
            meta=topics.CheckedTopic(topics.control_meta(device_name, control_name)),
            error=topics.CheckedTopic(topics.control_error(device_name, control_name)),
        )
        try:
            control_type = ControlType(control_type)
        except ValueError:
            expected = [item.value for item in ControlType]
            raise ValueError(f"unknown control type {control_type!r}, expected one of {expected}") from None
        device = self._require_own_device(device_name)
        existing = self.state.get_control(device_name, control_name)
        if existing is not None and existing.exists:
            raise ValueError(f"control {device_name}/{control_name} exists already")

        # Meta, by default the control goes last on the card
        fields = parse_meta_fields(meta_fields)
        if fields.get("order") is None:
            fields["order"] = device.next_order
        meta = ControlMeta(control_type=control_type, **fields)

        # Initial value
        payload, normalized_value = None, None
        if value is not None:
            if control_type == ControlType.PUSHBUTTON:
                # A press is an event, a button has no value to start with
                raise TypeError(f"a pushbutton takes no initial value, got {value!r}")
            payload, normalized_value = to_payload_and_value(meta, value)

        # Every check passed, the control goes into the state and the queue
        device.next_order = max(device.next_order, meta.order + 1)
        self.state.mark_control_added(device_name, control_name, meta)
        control = device.controls[control_name]
        control.topics = control_topics
        messages: list[tuple[str, Optional[str]]] = [(control.topics.meta, meta.to_json())]
        if payload is not None:
            control.payload = payload
            self.state.set_control_value(device_name, control_name, normalized_value)
            # Together with the meta, a reader needs no order between them
            messages.append((control.topics.value, payload))
        await self._wait_for_broker(self._publisher.queue_retained(messages))

    async def set_control_value(self, device_name: str, control_name: str, value: Any) -> None:
        self._raise_if_closed()
        control = self._require_existing_control(device_name, control_name)

        # Foreign control, a command to /on
        if not self.state.is_own_device(device_name):
            payload, _ = to_payload_and_value(control.meta, value)
            await self._publisher.publish(topics.control_command(device_name, control_name), payload, qos=1)
            return

        # Own pushbutton, a press is an event and keeps no value
        if control.meta.control_type == ControlType.PUSHBUTTON:
            payload, _ = to_payload_and_value(control.meta, value)
            generation = control.generation
            await self._publisher.publish(control.topics.value, payload, qos=1)
            # A press that did not reach the broker is not an event, nor one of a button removed meanwhile
            if (
                self.state.get_control(device_name, control_name) is control
                and control.generation == generation
            ):
                self.state.emit_pushbutton_press(device_name, control_name)
            return

        # Own control of another type, the payload goes out only when it changed
        if value is None:
            # Clears the value, the broker drops the retained message
            payload, normalized_value = None, None
        else:
            payload, normalized_value = to_payload_and_value(control.meta, value)
        changed = payload != control.payload
        control.payload = payload
        self.state.set_control_value(device_name, control_name, normalized_value)
        if changed:
            await self._publisher.publish(
                control.topics.value, payload, qos=1, retain=True, wait_for_puback=False
            )

    async def set_control_error(self, device_name: str, control_name: str, error: ControlErrorFlags) -> None:
        _check_error_flags(error)
        self._require_own_device(device_name)
        control = self._require_existing_control(device_name, control_name)
        if error == control.error:
            return
        self.state.set_control_error(device_name, control_name, error)
        await self._publish_error(control.topics.error, error)

    async def set_device_error(self, name: str, error: ControlErrorFlags) -> None:
        _check_error_flags(error)
        device = self._require_own_device(name)
        if error == device.error:
            return
        self.state.set_device_error(name, error)
        await self._publish_error(device.topics.error, error)

    async def update_control_meta(self, device_name: str, control_name: str, **fields: Any) -> None:
        self._require_own_device(device_name)
        control = self._require_existing_control(device_name, control_name)
        updated = dataclasses.replace(control.meta, **parse_meta_fields(fields))
        if updated == control.meta:
            return
        self.state.set_control_meta(device_name, control_name, updated)
        # Waits for PUBACK like the meta of the device
        await self._publisher.publish(control.topics.meta, updated.to_json(), qos=1, retain=True)

    async def remove_control(self, device_name: str, control_name: str) -> None:
        self._raise_if_closed()
        if self._is_removed(device_name):
            # remove() of the device erased its controls already
            return
        self._require_own_device(device_name)
        control = self.state.get_control(device_name, control_name)
        if control is None or not control.exists:
            return
        to_erase = self._add_own_topics_under(_get_control_topics_to_erase(control), control.topics.value)
        self.state.mark_control_removed(device_name, control_name)
        await self._wait_for_broker(self._erase(to_erase))

    async def remove_driver_devices(self) -> list[str]:
        self._raise_if_closed()
        # A create_device cancelled just before, as in a service that stops, still makes its device own
        creations = self._tasks.get_tasks_with_prefix(_CREATION_TASK_PREFIX)
        if creations:
            await asyncio.wait(creations)

        # Names of the driver in the broker
        signatures = await self._subscriber.read_retained(
            [topics.ALL_DEVICE_METAS, topics.ALL_LEGACY_DRIVERS]
        )
        # Own devices the broker may not have yet, the replay after reconnect is still on its way
        own = self.state.own_device_names()
        names = sorted({*_get_driver_device_names(self._driver, signatures), *own})

        # Own devices through remove_device, then whatever is left under the name
        for name in names:
            if self.state.is_own_device(name):
                await self.remove_device(name)
            leftovers = await self._subscriber.read_retained([topics.device_tree(name)])
            # A device the service created meanwhile is not a leftover
            if leftovers and not self.state.is_own_device(name):
                await self._wait_for_broker(self._erase_leftovers(leftovers))
        return names

    # ------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------

    def add_control_listener(self, device_name: str, control_name: str, event: str, handler: Handler) -> None:
        """
        Register a handler of a control event.

        on_command of a removed own control waits until the service creates it again.

        Raises:
            RuntimeError: the device is foreign and the client has not read it.
            NotOwnError: on_command of a foreign control.
            TypeError: the handler does not take the arguments of the event.
            ClientClosedError: the client is closed.
        """
        self._raise_if_closed()
        self._check_device_watched(device_name)
        if event == "command":
            self._check_takes_commands(device_name)
        self.state.add_control_listener(device_name, control_name, event, handler)

    def add_device_listener(self, device_name: str, event: str, handler: Handler) -> None:
        """
        Register a handler of an event of the device itself or of any of its controls.

        on_control_command of a removed own device waits until the service creates it again.

        Raises:
            RuntimeError: the device is foreign and the client has not read it.
            NotOwnError: on_control_command of a foreign device.
            TypeError: the handler does not take the arguments of the event.
            ClientClosedError: the client is closed.
        """
        self._raise_if_closed()
        self._check_device_watched(device_name)
        if event == "control_command":
            self._check_takes_commands(device_name)
        self.state.add_device_listener(device_name, event, handler)

    # ------------------------------------------------------------
    # Messages from the broker
    # ------------------------------------------------------------

    def _on_device_message(self, message: Message) -> None:
        """
        Apply a message of a device subscription. An own device is not followed, a reader of its name
        subscribed earlier still gets its echoes, they change nothing.
        """
        topic = topics.parse(message.topic)
        if topic is None or topic.kind is topics.TopicKind.COMMAND:
            return
        device = self.state.get_device(topic.device)
        if device is None or device.ownership is Ownership.OWN:
            return
        _apply_foreign_message(self.state, topic, message.payload.decode("utf-8", "replace"), message.retain)

    def _on_command(self, message: Message) -> None:
        # Commands to own devices only
        topic = topics.parse(message.topic)
        if (
            topic is None
            or topic.kind is not topics.TopicKind.COMMAND
            or not self.state.is_own_device(topic.device)
        ):
            return

        # A retained command, left from the past or published retained later, is not executed, it is erased
        # like libwbmqtt1 does. An empty one, the echo of such an erasure for example, is skipped.
        if message.retain:
            if message.payload:
                logger.debug("Ignoring retained command on %s and scheduling its erasure", message.topic)
                self._erase([message.topic])
            return

        # Skipped without a control, a payload or a handler
        control = self.state.get_control(topic.device, topic.control)
        payload = message.payload.decode("utf-8", "replace")
        if control is None or not control.exists:
            logger.debug("Command to %s/%s skipped, the control does not exist", topic.device, topic.control)
            return
        if not payload:
            return
        if not self.state.has_command_listeners(topic.device, topic.control):
            logger.debug("Command to %s/%s has no command handler", topic.device, topic.control)
            return

        # A press is always True, another command is parsed by the meta
        is_press = control.meta.control_type == ControlType.PUSHBUTTON
        value = True if is_press else parse_value(control.meta, payload)
        self.state.emit_command(topic.device, topic.control, value)

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------

    def _require_own_device(self, name: str) -> DeviceRecord:
        """
        Record of an own device for an action of the owner.

        Raises:
            ControlMissingError: the service removed the device.
            NotOwnError: the device is foreign.
            ClientClosedError: the client is closed.
        """
        self._raise_if_closed()
        if self._is_removed(name):
            raise ControlMissingError(f"device {name} is removed")
        device = self.state.get_device(name)
        if device is None or device.ownership is not Ownership.OWN:
            raise NotOwnError(f"{name} is not a device of this client")
        return device

    def _is_removed(self, name: str) -> bool:
        device = self.state.get_device(name)
        return device is not None and device.ownership is Ownership.REMOVED

    def _check_takes_commands(self, name: str) -> None:
        """
        Commands come to own devices, a removed one keeps its command handlers until it is created again.

        Raises:
            NotOwnError: the device is foreign.
        """
        if not (self.state.is_own_device(name) or self._is_removed(name)):
            raise NotOwnError(f"{name} is not a device of this client")

    def _check_device_watched(self, name: str) -> None:
        """
        Make sure events of the device reach this client, otherwise a handler would never be called.

        Raises:
            RuntimeError: the device is foreign and the client has not read it through get_device
                or get_control.
        """
        device = self.state.get_device(name)
        if device is not None and device.ownership is not Ownership.FOREIGN:
            return
        if name not in self._device_watches:
            raise RuntimeError(
                f"device {name} is not read by this client, call get_device or get_control first"
            )

    def _require_existing_control(self, device_name: str, control_name: str) -> ControlRecord:
        control = self.state.get_control(device_name, control_name)
        if control is None or not control.exists:
            raise ControlMissingError(f"{device_name}/{control_name} does not exist")
        return control

    def _get_device_meta_json(self, device: DeviceRecord) -> str:
        return DeviceMeta(device.title, self._driver).to_json()

    async def _publish_error(self, topic: str, error: ControlErrorFlags) -> None:
        await self._publisher.publish(
            topic, error.to_mqtt() or None, qos=1, retain=True, wait_for_puback=False
        )

    def _add_own_topics_under(self, to_erase: list[str], root: str) -> list[str]:
        """
        The topics to erase and after them every other topic the own state publishes under root, legacy
        meta subtopics for example.
        """
        listed = set(to_erase)
        return to_erase + [
            topic for topic in self._publisher.get_own_topics_under(root) if topic not in listed
        ]

    def _erase(self, to_erase: Iterable[str]) -> asyncio.Future:
        """
        Erase retained topics in the stages of topics.erasure_order, keeping the given order inside a stage.
        """
        staged = sorted(
            ((topic, topics.erasure_order(topic)[0]) for topic in to_erase), key=lambda item: item[1]
        )
        return self._publisher.queue_erasure(staged)

    def _erase_leftovers(self, leftovers: dict[str, bytes]) -> asyncio.Future:
        return self._erase(sorted(leftovers, key=topics.erasure_order))

    async def _subscribe_commands(self, name: str) -> None:
        try:
            await self._subscriber.subscribe([topics.command_filter(name)], self._on_command)
        except (BrokerDisconnectedError, ClientClosedError):
            # Without a link it is subscribed all the same, on close it does not matter
            pass

    async def _unsubscribe_commands_after(self, name: str, erasure_done: asyncio.Future) -> None:
        """
        Unsubscribe from the commands of a removed device once its erasure is through, unless the name
        is own again by then and its new device takes the subscription over.
        """
        await erasure_done
        await self._subscriber.unsubscribe(
            topics.command_filter(name), self._on_command, still_needed=lambda: self.state.is_own_device(name)
        )

    async def _wait_for_broker(self, pending: asyncio.Future) -> None:
        """
        Wait for the broker side of an operation that is in the state already and runs on its own.

        A cancelled caller stops waiting, the operation goes on. When close() ends it the caller gets
        ClientClosedError.
        """
        pending.add_done_callback(retrieve_error)
        try:
            await asyncio.shield(pending)
        except asyncio.CancelledError:
            # The internal task the caller waits for was cancelled by close(), not the caller itself
            if pending.done() and self._is_closed() and not cancelled_from_outside():
                raise ClientClosedError("the client is closed") from None
            raise

    def _raise_if_closed(self) -> None:
        if self._is_closed():
            raise ClientClosedError("the client is closed")


def _check_not_created(name: str, device: DeviceRecord) -> None:
    if device.ownership is Ownership.OWN:
        raise ValueError(f"device {name} exists already")


def _check_error_flags(error: Any) -> None:
    if not isinstance(error, ControlErrorFlags):
        raise TypeError(f"error must be ControlErrorFlags, got {error!r}")


def _get_control_topics_to_erase(control: ControlRecord) -> list[str]:
    """
    Topics an own control publishes, in the order they are erased. A pushbutton keeps no value.
    """
    to_erase = [control.topics.error, control.topics.meta]
    if control.meta.control_type != ControlType.PUSHBUTTON:
        to_erase.insert(0, control.topics.value)
    return to_erase


def _get_device_driver(name: str, retained: dict[str, bytes]) -> Optional[str]:
    """
    Driver that signs the device. It comes from the JSON meta, read as a live reader reads it, or from
    the legacy /meta/driver when the JSON meta names none.
    """
    meta = retained.get(topics.device_meta(name))
    parsed = parse_device_meta(meta.decode("utf-8", "replace")) if meta else None
    if parsed is not None and parsed.driver:
        return parsed.driver
    legacy = retained.get(topics.legacy_driver(name))
    return legacy.decode("utf-8", "replace") if legacy else None


def _get_device_signature(name: str, retained: dict[str, bytes]) -> tuple[bool, Optional[str]]:
    """
    Whether the retained topics of a name make a device and which driver signs it.

    Topics without a JSON meta, a /meta/driver and a control meta are leftovers that sign nothing.
    """
    signed = _build_snapshot(name, retained) is not None or topics.legacy_driver(name) in retained
    return signed, _get_device_driver(name, retained)


def _get_driver_device_names(driver: str, signatures: dict[str, bytes]) -> set[str]:
    """
    Names of the devices signed by the driver, from JSON device metas and legacy /meta/driver subtopics.
    """
    names = {topics.device_name(topic) for topic in signatures}
    return {name for name in names if _get_device_driver(name, signatures) == driver}


def _apply_foreign_message(state: State, topic: topics.ParsedTopic, payload: str, retain: bool) -> None:
    """
    Apply a message of a foreign device to the state. retain is the flag the message was published with.
    """
    # Meta of the device, an empty one clears it
    if topic.kind is topics.TopicKind.DEVICE_META:
        if not payload:
            state.clear_device_meta(topic.device)
            return
        meta = parse_device_meta(payload)
        if meta is not None:
            state.set_device_meta(topic.device, meta)
        return

    # Error of the device itself
    if topic.kind is topics.TopicKind.DEVICE_ERROR:
        state.set_device_error(topic.device, ControlErrorFlags.from_mqtt(payload))
        return

    # Value, meta and error of a control
    if topic.kind is topics.TopicKind.VALUE:
        _apply_value(state, topic.device, topic.control, payload, retain)
    elif topic.kind is topics.TopicKind.META:
        _apply_control_meta(state, topic.device, topic.control, payload)
    elif topic.kind is topics.TopicKind.ERROR:
        _apply_control_error(state, topic.device, topic.control, payload)


def _apply_control_meta(state: State, device_name: str, control_name: str, payload: str) -> None:
    # An empty meta removes the control
    control = state.get_control(device_name, control_name)
    if not payload:
        if control is not None and control.exists:
            state.mark_control_removed(device_name, control_name)
        else:
            state.drop_control_if_unused(device_name, control_name)
        return

    # A meta that does not parse changes nothing
    meta, error = parse_control_meta(payload)
    if meta is None:
        logger.debug("Meta of %s/%s is not usable: %r", device_name, control_name, payload[:200])
        return

    # The first meta adds the control, a later one changes it, an error may come with it
    control = state.ensure_control(device_name, control_name)
    if control.meta is None:
        state.mark_control_added(device_name, control_name, meta)
    elif meta != control.meta:
        state.set_control_meta(device_name, control_name, meta)
    if error is not None:
        state.set_control_error(device_name, control_name, error)


def _apply_control_error(state: State, device_name: str, control_name: str, payload: str) -> None:
    # A payload without the letters r, w and p is no error either
    error = ControlErrorFlags.from_mqtt(payload)
    # No error of a control the state does not know, nothing to keep
    if state.get_control(device_name, control_name) is None and not error:
        return

    state.ensure_control(device_name, control_name)
    state.set_control_error(device_name, control_name, error)
    # A cleared error may leave a record with nothing in it
    if not error:
        state.drop_control_if_unused(device_name, control_name)


def _apply_value(state: State, device_name: str, control_name: str, payload: str, retain: bool) -> None:
    # A value may come before the meta, the record of the control keeps it until then
    control = state.get_control(device_name, control_name)
    if control is None:
        if not payload:
            return
        control = state.ensure_control(device_name, control_name)

    # A press is an event. A retained message here is a leftover, not a press.
    if control.meta is not None and control.meta.control_type == ControlType.PUSHBUTTON:
        if payload and not retain:
            state.emit_pushbutton_press(device_name, control_name)
        return

    # The payload is kept, the value is parsed by the meta, now or once the meta comes
    control.payload = payload or None
    if control.meta is not None:
        state.set_control_value(
            device_name, control_name, parse_value(control.meta, payload) if payload else None
        )
    elif not payload:
        state.drop_control_if_unused(device_name, control_name)


def _build_state(retained: dict[str, bytes]) -> State:
    """
    State of their own built from retained topics by the code that follows foreign devices, so a one-off
    read sees exactly what a live reader sees.
    """
    state = State(ListenerRegistry(), lambda device, control: None, lambda device: None)
    for topic, payload in retained.items():
        parsed = topics.parse(topic)
        if parsed is not None and parsed.kind is not topics.TopicKind.COMMAND:
            _apply_foreign_message(state, parsed, payload.decode("utf-8", "replace"), retain=True)
    return state


def _build_snapshot(name: str, retained: dict[str, bytes]) -> Optional[DeviceSnapshot]:
    """
    Device from the retained topics of one name, as a reader sees it. None when they make no device.
    """
    state = _build_state(retained)
    device = state.get_device(name)
    if device is None or not device.exists:
        return None
    controls = {
        control_name: ControlSnapshot(control.meta, control.value, control.error)
        for control_name, control in device.controls.items()
        if control.exists
    }
    return DeviceSnapshot(name, device.title, device.driver, controls, device.error)
