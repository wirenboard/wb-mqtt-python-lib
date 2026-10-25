"""
Device sync of a client, keeps the state of devices in sync with the broker.

It subscribes to foreign devices and applies their messages, publishes own devices and takes commands to them.
Own devices are not followed in the broker, an outside change of one lasts until the client publishes it
again after reconnect. The state itself lives in state.py.

An operation on an own device changes the state and the mirror of the publisher in one step, without
an await in between, and only then waits for the broker. A cancelled caller stops waiting, the change still
goes out whole, and another task never sees it half done.
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
from .topics import CheckedTopic
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
    from_payload,
    parse_control_meta,
    parse_device_meta,
    parse_meta_fields,
    to_payload_and_value,
)

logger = logging.getLogger(__name__)

# Name prefix of the tasks that finish create_device
_CREATION = "create "


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
        # Foreign reading: subscription to the whole device, kept until the client closes
        self._watches: dict[str, asyncio.Task] = {}
        self._tasks = TaskSet(logger)

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    async def close(self) -> None:
        """
        End the waits with ClientClosedError and stop subscriptions and erasures in progress.
        """
        self.state.fail_waits(ClientClosedError("the client is closed"))
        await self._tasks.cancel_all(self._watches.values())

    # Foreign devices

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
        if device.ownership is Ownership.OWN:
            return
        watch = self._watches.get(name)
        if watch is None:
            watch = asyncio.create_task(self._subscribe_device(name), name=f"subscribe {name}")
            watch.add_done_callback(lambda task: self._forget_failed_watch(name, task))
            self._watches[name] = watch
        await self._wait_for_broker(watch)

    def _forget_failed_watch(self, name: str, watch: asyncio.Task) -> None:
        # The next call subscribes again, also when every caller of this one was cancelled meanwhile
        if not watch.cancelled() and watch.exception() is not None and self._watches.get(name) is watch:
            del self._watches[name]

    async def _subscribe_device(self, name: str) -> None:
        try:
            await self._subscriber.subscribe(topics.watch_filters(name), self._on_device_message)
        except BaseException:
            # The next call subscribes again
            for topic_filter in topics.watch_filters(name):
                await self._subscriber.unsubscribe(topic_filter, self._on_device_message)
            raise

    async def wait_device_exists(self, name: str, timeout: float) -> None:
        self._raise_if_closed()
        await self.state.wait_device_exists(name, timeout)

    async def wait_control_exists(self, device_name: str, control_name: str, timeout: float) -> None:
        self._raise_if_closed()
        await self.state.wait_control_exists(device_name, control_name, timeout)

    async def wait_value(self, device_name: str, control_name: str, expected: Any, timeout: float) -> Any:
        self._raise_if_closed()
        self._check_watched(device_name)
        return await self.state.wait_value(device_name, control_name, expected, timeout)

    # Own devices

    async def create_device(self, name: str, title: TitleLike, force: bool = False) -> None:
        self._raise_if_closed()
        check_name(name, "device")
        title = TranslatedTitle.parse(title)
        prepared = topics.DeviceTopics(
            meta=CheckedTopic(topics.device_meta(name)),
            error=CheckedTopic(topics.device_error(name)),
        )
        device = self.state.ensure_device(name)
        _check_not_created(name, device)
        leftovers = await self._subscriber.read_retained([topics.device_tree(name)])
        # Another create_device of this client may have finished meanwhile, its device is not a leftover
        _check_not_created(name, device)
        signed, owner = _signature_of(name, leftovers)
        foreign = signed and owner != self._driver
        if foreign and not force:
            raise DeviceTakenError(f"device {name} belongs to driver {owner!r}")
        if foreign:
            logger.debug("Device %s of driver %r is replaced by force", name, owner)
        elif leftovers:
            logger.debug("Device %s is left from a previous run, erasing %d topics", name, len(leftovers))
        # A cancelled caller stops waiting, the device still becomes own
        created = self._tasks.start(
            self._take_over_name(name, title, prepared, leftovers), f"{_CREATION}{name}", report=False
        )
        await self._wait_for_broker(created)

    async def _take_over_name(
        self, name: str, title: TranslatedTitle, prepared: topics.DeviceTopics, leftovers: dict[str, bytes]
    ) -> None:
        """
        Erase the leftovers under the name and publish the own device in their place.

        Once the name is checked, the device becomes own also without a link. Its erasures and meta wait
        in the mirror in their order and go out after reconnect, like any own change.
        """
        if leftovers:
            await self._erase_leftovers(leftovers)
        device = self.state.ensure_device(name)
        _check_not_created(name, device)
        self.state.make_own(name, title, self._driver)
        device.topics = prepared
        published = self._publisher.queue_retained([(device.topics.meta, self._device_meta_json(device))])
        subscribed = self._tasks.start(self._subscribe_commands(name), f"subscribe commands of {name}")
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
        await self._publisher.publish(device.topics.meta, self._device_meta_json(device), qos=1, retain=True)

    async def remove_device(self, name: str) -> None:
        self._raise_if_closed()
        if self._is_removed(name):
            return
        device = self._require_own_device(name)
        if any(control.listeners.has("command") for control in device.controls.values()) or (
            device.listeners.has("control_command")
        ):
            logger.debug("Device %s is removed, its command handlers wait until it is created again", name)
        erased = [
            topic
            for control_name in reversed(self.state.existing_control_names(name))
            for topic in _control_topics_to_erase(device.controls[control_name])
        ]
        if device.error:
            erased.append(device.topics.error)
        erased.append(device.topics.meta)
        # A reader of the name applies the echoes of the erasure like those of a foreign device
        self.state.mark_removed(name)
        erasure_done = self._erase(self._with_published_under(erased, topics.device_root(name)))
        await self._wait_for_broker(
            self._tasks.start(self._unsubscribe_commands_after(name, erasure_done), f"remove {name}")
        )

    async def create_control(  # pylint: disable=too-many-arguments,too-many-locals
        self, device_name: str, control_name: str, control_type: str, value: Any = None, **meta_fields: Any
    ) -> None:
        self._raise_if_closed()
        check_name(control_name, "control")
        prepared = topics.ControlTopics(
            value=CheckedTopic(topics.control_value(device_name, control_name)),
            meta=CheckedTopic(topics.control_meta(device_name, control_name)),
            error=CheckedTopic(topics.control_error(device_name, control_name)),
        )
        try:
            control_type = ControlType(control_type)
        except ValueError:
            expected = [item.value for item in ControlType]
            raise ValueError(f"unknown control type {control_type!r}, expected one of {expected}") from None
        device = self._require_own_device(device_name)
        existing = self.state.control(device_name, control_name)
        if existing is not None and existing.exists:
            raise ValueError(f"control {device_name}/{control_name} exists already")
        fields = parse_meta_fields(meta_fields)
        if fields.get("order") is None:
            fields["order"] = device.next_order
        meta = ControlMeta(control_type=control_type, **fields)
        payload, normalized_value = None, None
        if value is not None:
            if control_type == ControlType.PUSHBUTTON:
                # A press is an event, a button has no value to start with
                raise TypeError(f"a pushbutton takes no initial value, got {value!r}")
            payload, normalized_value = to_payload_and_value(meta, value)
        device.next_order = max(device.next_order, meta.order + 1)
        self.state.mark_control_added(device_name, control_name, meta)
        control = device.controls[control_name]
        control.topics = prepared
        messages: list[tuple[str, Optional[str]]] = [(control.topics.meta, meta.to_json())]
        if payload is not None:
            control.payload = payload
            self.state.set_value(device_name, control_name, normalized_value)
            # Together with the meta, a reader needs no order between them
            messages.append((control.topics.value, payload))
        await self._wait_for_broker(self._publisher.queue_retained(messages))

    async def set_value(self, device_name: str, control_name: str, value: Any) -> None:
        self._raise_if_closed()
        control = self._require_existing_control(device_name, control_name)
        if not self.state.is_own(device_name):
            payload, _ = to_payload_and_value(control.meta, value)
            await self._publisher.publish(topics.control_command(device_name, control_name), payload, qos=1)
            return
        if control.meta.control_type == ControlType.PUSHBUTTON:
            payload, _ = to_payload_and_value(control.meta, value)
            generation = control.generation
            await self._publisher.publish(control.topics.value, payload, qos=1)
            # A press that did not reach the broker is not an event, nor one of a button removed meanwhile
            if self.state.control(device_name, control_name) is control and control.generation == generation:
                self.state.emit_press(device_name, control_name)
            return
        if value is None:
            # Clears the value, the broker drops the retained message
            payload, normalized_value = None, None
        else:
            payload, normalized_value = to_payload_and_value(control.meta, value)
        publish = payload != control.payload
        control.payload = payload
        self.state.set_value(device_name, control_name, normalized_value)
        if publish:
            await self._publisher.publish(
                control.topics.value, payload, qos=1, retain=True, wait_for_puback=False
            )

    async def set_error(self, device_name: str, control_name: str, error: ControlErrorFlags) -> None:
        _check_error_flags(error)
        self._require_own_device(device_name)
        control = self._require_existing_control(device_name, control_name)
        if error == control.error:
            return
        self.state.set_error(device_name, control_name, error)
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
        control = self.state.control(device_name, control_name)
        if control is None or not control.exists:
            return
        erased = self._with_published_under(_control_topics_to_erase(control), control.topics.value)
        self.state.mark_control_removed(device_name, control_name)
        await self._wait_for_broker(self._erase(erased))

    async def remove_driver_devices(self) -> list[str]:
        self._raise_if_closed()
        creations = self._tasks.named(_CREATION)
        if creations:
            # A create_device cancelled just before, as in a service that stops, still makes its device own
            await asyncio.wait(creations)
        signatures = await self._subscriber.read_retained(
            [topics.ALL_DEVICE_METAS, topics.ALL_LEGACY_DRIVERS]
        )
        # Own devices the broker may not have yet, the replay after reconnect is still on its way
        own = self.state.own_device_names()
        names = sorted({*_device_names_of_driver(self._driver, signatures), *own})
        for name in names:
            if self.state.is_own(name):
                await self.remove_device(name)
            leftovers = await self._subscriber.read_retained([topics.device_tree(name)])
            # A device the service created meanwhile is not a leftover
            if leftovers and not self.state.is_own(name):
                await self._wait_for_broker(self._erase_leftovers(leftovers))
        return names

    async def get_device_names(self) -> list[str]:
        self._raise_if_closed()
        retained = await self._subscriber.read_retained([topics.ALL_DEVICE_METAS, topics.ALL_CONTROL_METAS])
        return _state_of(retained).existing_device_names()

    async def get_device_snapshot(self, name: str) -> Optional[DeviceSnapshot]:
        self._raise_if_closed()
        check_name(name, "device")
        return _snapshot_of(name, await self._subscriber.read_retained([topics.device_tree(name)]))

    # Listeners

    def add_control_listener(self, device_name: str, control_name: str, kind: str, handler: Handler) -> None:
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
        self._check_watched(device_name)
        if kind == "command":
            self._check_takes_commands(device_name)
        self.state.add_control_listener(device_name, control_name, kind, handler)

    def add_device_listener(self, device_name: str, kind: str, handler: Handler) -> None:
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
        self._check_watched(device_name)
        if kind == "control_command":
            self._check_takes_commands(device_name)
        self.state.add_device_listener(device_name, kind, handler)

    # Messages from the broker

    def _on_device_message(self, message: Message) -> None:
        """
        Apply a message of a device subscription. An own device is not followed, a reader of its name
        subscribed earlier still gets its echoes, they change nothing.
        """
        topic = topics.parse(message.topic)
        if topic is None or topic.kind is topics.TopicKind.COMMAND:
            return
        device = self.state.device(topic.device)
        if device is None or device.ownership is Ownership.OWN:
            return
        _apply_foreign_message(self.state, topic, message.payload.decode("utf-8", "replace"), message.retain)

    def _on_command(self, message: Message) -> None:
        topic = topics.parse(message.topic)
        if topic is None or topic.kind is not topics.TopicKind.COMMAND or not self.state.is_own(topic.device):
            return
        if message.retain:
            # A command left from the past is not executed, it is erased like libwbmqtt1 does
            if message.payload:
                logger.debug("Ignoring retained command on %s and scheduling its erasure", message.topic)
                self._erase([message.topic])
            return
        control = self.state.control(topic.device, topic.control)
        payload = message.payload.decode("utf-8", "replace")
        if control is None or not control.exists:
            logger.debug("Command to %s/%s skipped, the control does not exist", topic.device, topic.control)
            return
        if not payload:
            return
        if not self.state.has_command_listeners(topic.device, topic.control):
            logger.debug("Command to %s/%s has no command handler", topic.device, topic.control)
            return
        is_press = control.meta.control_type == ControlType.PUSHBUTTON
        value = True if is_press else from_payload(control.meta, payload)
        self.state.emit_command(topic.device, topic.control, value)

    # Helpers

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
        device = self.state.device(name)
        if device is None or device.ownership is not Ownership.OWN:
            raise NotOwnError(f"{name} is not a device of this client")
        return device

    def _is_removed(self, name: str) -> bool:
        device = self.state.device(name)
        return device is not None and device.ownership is Ownership.REMOVED

    def _check_takes_commands(self, name: str) -> None:
        """
        Commands come to own devices, a removed one keeps its command handlers until it is created again.

        Raises:
            NotOwnError: the device is foreign.
        """
        if not (self.state.is_own(name) or self._is_removed(name)):
            raise NotOwnError(f"{name} is not a device of this client")

    def _check_watched(self, name: str) -> None:
        """
        Make sure events of the device reach this client, otherwise a handler would never be called.

        Raises:
            RuntimeError: the device is foreign and the client has not read it through get_device
                or get_control.
        """
        device = self.state.device(name)
        if device is not None and device.ownership is not Ownership.FOREIGN:
            return
        if name not in self._watches:
            raise RuntimeError(
                f"device {name} is not read by this client, call get_device or get_control first"
            )

    def _require_existing_control(self, device_name: str, control_name: str) -> ControlRecord:
        control = self.state.control(device_name, control_name)
        if control is None or not control.exists:
            raise ControlMissingError(f"{device_name}/{control_name} does not exist")
        return control

    def _device_meta_json(self, device: DeviceRecord) -> str:
        return DeviceMeta(device.title, self._driver).to_json()

    async def _publish_error(self, topic: str, error: ControlErrorFlags) -> None:
        await self._publisher.publish(
            topic, error.to_mqtt() or None, qos=1, retain=True, wait_for_puback=False
        )

    def _with_published_under(self, erased: list[str], root: str) -> list[str]:
        """
        The topics to erase and after them what else the own state publishes under root, legacy meta
        subtopics for example.
        """
        listed = set(erased)
        return erased + [topic for topic in self._publisher.retained_under(root) if topic not in listed]

    def _erase(self, erased: Iterable[str]) -> asyncio.Future:
        """
        Erase retained topics in the stages of topics.erasure_order, keeping the given order inside a stage.
        """
        staged = sorted(
            ((topic, topics.erasure_order(topic)[0]) for topic in erased), key=lambda item: item[1]
        )
        return self._publisher.queue_erasure(staged)

    def _erase_leftovers(self, retained: dict[str, bytes]) -> asyncio.Future:
        return self._erase(sorted(retained, key=topics.erasure_order))

    async def _subscribe_commands(self, name: str) -> None:
        try:
            await self._subscriber.subscribe([topics.command_filter(name)], self._on_command)
        except BrokerDisconnectedError:
            # Subscribed all the same, the broker gets it after reconnect
            pass
        except ClientClosedError:
            # The client closes, the subscription does not matter any more
            pass

    async def _unsubscribe_commands_after(self, name: str, erasure_done: asyncio.Future) -> None:
        """
        Unsubscribe from the commands of a removed device once its erasure is through, unless the name
        is own again by then and its new device takes the subscription over.
        """
        await erasure_done
        await self._subscriber.unsubscribe(
            topics.command_filter(name), self._on_command, keep=lambda: self.state.is_own(name)
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


def _control_topics_to_erase(control: ControlRecord) -> list[str]:
    """
    Topics an own control publishes, in the order they are erased. A pushbutton keeps no value.
    """
    erased = [control.topics.error, control.topics.meta]
    if control.meta.control_type != ControlType.PUSHBUTTON:
        erased.insert(0, control.topics.value)
    return erased


def _driver_of(name: str, retained: dict[str, bytes]) -> Optional[str]:
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


def _signature_of(name: str, retained: dict[str, bytes]) -> tuple[bool, Optional[str]]:
    """
    Whether the retained topics of a name make a device and which driver signs it.

    Topics without a JSON meta, a /meta/driver and a control meta are leftovers that sign nothing.
    """
    signed = _snapshot_of(name, retained) is not None or topics.legacy_driver(name) in retained
    return signed, _driver_of(name, retained)


def _device_names_of_driver(driver: str, signatures: dict[str, bytes]) -> set[str]:
    """
    Names of the devices signed by the driver, from JSON device metas and legacy /meta/driver subtopics.
    """
    names = {topics.device_name(topic) for topic in signatures}
    return {name for name in names if _driver_of(name, signatures) == driver}


def _apply_foreign_message(state: State, topic: topics.ParsedTopic, payload: str, retain: bool) -> None:
    """
    Apply a message of a foreign device to the state. retain is True for what the broker kept.
    """
    if topic.kind is topics.TopicKind.DEVICE_META:
        if not payload:
            state.clear_device_meta(topic.device)
            return
        meta = parse_device_meta(payload)
        if meta is not None:
            state.set_device_meta(topic.device, meta)
        return
    if topic.kind is topics.TopicKind.DEVICE_ERROR:
        state.set_device_error(topic.device, ControlErrorFlags.from_mqtt(payload))
        return
    if topic.kind is topics.TopicKind.VALUE:
        _apply_value(state, topic.device, topic.control, payload, retain)
    elif topic.kind is topics.TopicKind.META:
        _apply_control_meta(state, topic.device, topic.control, payload)
    elif topic.kind is topics.TopicKind.ERROR:
        # A payload without the letters r, w and p is no error either
        error = ControlErrorFlags.from_mqtt(payload)
        if state.control(topic.device, topic.control) is None and not error:
            return
        state.ensure_control(topic.device, topic.control)
        state.set_error(topic.device, topic.control, error)
        if not error:
            state.drop_control_if_unused(topic.device, topic.control)


def _apply_control_meta(state: State, device_name: str, control_name: str, payload: str) -> None:
    control = state.control(device_name, control_name)
    if not payload:
        if control is not None and control.exists:
            state.mark_control_removed(device_name, control_name)
        else:
            state.drop_control_if_unused(device_name, control_name)
        return
    meta, error = parse_control_meta(payload)
    if meta is None:
        logger.debug("Meta of %s/%s is not usable: %r", device_name, control_name, payload[:200])
        return
    control = state.ensure_control(device_name, control_name)
    if control.meta is None:
        state.mark_control_added(device_name, control_name, meta)
    elif meta != control.meta:
        state.set_control_meta(device_name, control_name, meta)
    if error is not None:
        state.set_error(device_name, control_name, error)


def _apply_value(state: State, device_name: str, control_name: str, payload: str, retain: bool) -> None:
    control = state.control(device_name, control_name)
    if control is None:
        if not payload:
            return
        control = state.ensure_control(device_name, control_name)
    if control.meta is not None and control.meta.control_type == ControlType.PUSHBUTTON:
        # A press is an event. A retained message here is a leftover, not a press.
        if payload and not retain:
            state.emit_press(device_name, control_name)
        return
    control.payload = payload or None
    if control.meta is not None:
        state.set_value(device_name, control_name, from_payload(control.meta, payload) if payload else None)
    elif not payload:
        state.drop_control_if_unused(device_name, control_name)


def _state_of(retained: dict[str, bytes]) -> State:
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


def _snapshot_of(name: str, retained: dict[str, bytes]) -> Optional[DeviceSnapshot]:
    """
    Device from the retained topics of one name, as a reader sees it. None when they make no device.
    """
    state = _state_of(retained)
    device = state.device(name)
    if device is None or not device.exists:
        return None
    controls = {
        control_name: ControlSnapshot(control.meta, control.value, control.error)
        for control_name, control in device.controls.items()
        if control.exists
    }
    return DeviceSnapshot(name, device.title, device.driver, controls, device.error)
