"""
State of own and foreign devices of one client, by name.

Every change the listeners can see goes through here, becomes events for them and wakes the waits.
device_manager.py sets the fields nobody listens to itself, the topics of own records, next_order and
the last payload. Nothing here talks to the broker, device_manager.py does that.
"""

import asyncio
import enum
import math
from typing import Any, Callable, Optional, Union

from .._listeners import Handler, ListenerRegistry, ListenerSet, check_handler_arguments
from .._logs import get_logger
from .topics import ControlTopics, DeviceTopics
from .values import (
    ControlErrorFlags,
    ControlMeta,
    ControlType,
    DeviceMeta,
    TranslatedTitle,
    parse_value,
)

logger = get_logger(__name__)

# Control events that the device repeats to its own listeners
_DEVICE_EVENT_OF = {
    "added": "control_added",
    "removed": "control_removed",
    "value": "control_value",
    "error": "control_error",
    "meta": "control_meta",
    "command": "control_command",
}
_CONTROL_EVENT_OF = {device_event: event for event, device_event in _DEVICE_EVENT_OF.items()}
# Arguments each event brings to its handler
_HANDLER_ARGUMENTS = {"added": 1, "removed": 1, "meta": 1, "command": 2, "value": 3, "error": 3}
# Events of the device itself, they bring the device instead of a control
_DEVICE_SELF_EVENTS = frozenset({"added", "removed", "meta", "error"})


class Ownership(enum.Enum):
    """
    Whose a device name is for this client.

    REMOVED is a device the service removed itself. It reads like a foreign one, a second remove() does
    nothing and create_control says the device is removed.
    """

    FOREIGN = "foreign"
    OWN = "own"
    REMOVED = "removed"


# A wait for a value, the condition it waits for and the future it ends
ValueWait = tuple[Callable[[Any], bool], asyncio.Future]
# What the listeners of a device itself see, whether it exists, its title and its driver
_DeviceState = tuple[bool, TranslatedTitle, Optional[str]]


class ControlRecord:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """
    State of one control.
    """

    def __init__(self, listeners: ListenerSet) -> None:
        self.meta: Optional[ControlMeta] = None
        self.topics: Optional[ControlTopics] = None
        # The last payload, published for an own control and received for a foreign one, also before
        # the meta arrives. A new type reads it again.
        self.payload: Optional[str] = None
        self.value: Any = None
        self.error = ControlErrorFlags.NONE
        self.listeners = listeners
        self.value_waits: list[ValueWait] = []
        self.exists_waits: list[asyncio.Future] = []
        # Changes on every appearance and removal, so a wait can tell the control it started with
        self.generation = 0

    @property
    def exists(self) -> bool:
        """
        True while the control has meta with a type.
        """
        return self.meta is not None

    @property
    def is_unused(self) -> bool:
        """
        True when the record holds nothing, neither meta, value, error, listeners nor waits.
        """
        return (
            self.meta is None
            and self.payload is None
            and not self.error
            and not self.listeners
            and not self.value_waits
            and not self.exists_waits
        )


class DeviceRecord:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """
    State of one device.
    """

    def __init__(self, listeners: ListenerSet) -> None:
        self.ownership = Ownership.FOREIGN
        self.topics: Optional[DeviceTopics] = None
        self.has_meta = False
        self.title = TranslatedTitle()
        self.driver: Optional[str] = None
        self.error = ControlErrorFlags.NONE
        self.controls: dict[str, ControlRecord] = {}
        self.next_order = 1
        self.listeners = listeners
        self.exists_waits: list[asyncio.Future] = []

    @property
    def exists(self) -> bool:
        """
        True while the device has JSON meta or at least one control.
        """
        return self.has_meta or any(control.exists for control in self.controls.values())


class State:  # pylint: disable=too-many-public-methods
    """
    Records of devices and controls with their listeners and waits.

    Args:
        listener_registry: where the listeners of the client live.
        make_control: builds the Control object a handler gets, from a device and a control name.
        make_device: builds the Device object a handler gets, from a device name.
    """

    def __init__(
        self,
        listener_registry: ListenerRegistry,
        make_control: Callable[[str, str], Any],
        make_device: Callable[[str], Any],
    ) -> None:
        self._listener_registry = listener_registry
        self._make_control = make_control
        self._make_device = make_device
        self._devices: dict[str, DeviceRecord] = {}

    @property
    def wait_count(self) -> int:
        """
        Waits for a value or for appearance that still run, for leak checks.
        """
        count = 0
        for device in self._devices.values():
            count += len(device.exists_waits)
            for control in device.controls.values():
                count += len(control.value_waits) + len(control.exists_waits)
        return count

    # ------------------------------------------------------------
    # Records and names of devices and controls
    # ------------------------------------------------------------

    def get_device(self, name: str) -> Optional[DeviceRecord]:
        return self._devices.get(name)

    def get_control(self, device_name: str, control_name: str) -> Optional[ControlRecord]:
        device = self._devices.get(device_name)
        return device.controls.get(control_name) if device else None

    def ensure_device(self, name: str) -> DeviceRecord:
        device = self._devices.get(name)
        if device is None:
            device = DeviceRecord(ListenerSet(self._listener_registry, name))
            self._devices[name] = device
        return device

    def ensure_control(self, device_name: str, control_name: str) -> ControlRecord:
        device = self.ensure_device(device_name)
        control = device.controls.get(control_name)
        if control is None:
            control = ControlRecord(ListenerSet(self._listener_registry, f"{device_name}/{control_name}"))
            device.controls[control_name] = control
        return control

    def existing_device_names(self) -> list[str]:
        return sorted(name for name, device in self._devices.items() if device.exists)

    def own_device_names(self) -> list[str]:
        return sorted(name for name, device in self._devices.items() if device.ownership is Ownership.OWN)

    def existing_control_names(self, device_name: str) -> list[str]:
        device = self._devices.get(device_name)
        if device is None:
            return []
        return [name for name, control in device.controls.items() if control.exists]

    def is_own_device(self, device_name: str) -> bool:
        device = self._devices.get(device_name)
        return device is not None and device.ownership is Ownership.OWN

    # ------------------------------------------------------------
    # Waits until a device or a control appears or a value fits
    # ------------------------------------------------------------

    async def wait_device_exists(self, device_name: str, timeout: float) -> None:
        """
        Wait until the device exists.

        Raises:
            TimeoutError: it did not appear in time.
            ClientClosedError: the client closed while waiting.
        """
        await _wait_exists(self.ensure_device(device_name), timeout)

    async def wait_control_exists(self, device_name: str, control_name: str, timeout: float) -> None:
        """
        Wait until the control exists, see wait_device_exists.
        """
        try:
            await _wait_exists(self.ensure_control(device_name, control_name), timeout)
        finally:
            self.drop_control_if_unused(device_name, control_name)

    async def wait_control_value(
        self, device_name: str, control_name: str, expected: Any, timeout: float
    ) -> Any:
        # The value fits already
        control = self.ensure_control(device_name, control_name)
        predicate = expected if callable(expected) else (lambda value: value == expected)
        if control.value is not None and predicate(control.value):
            return control.value

        # Otherwise _wake_value_waits ends the wait when a value that fits comes
        value_wait = (predicate, asyncio.get_running_loop().create_future())
        control.value_waits.append(value_wait)
        try:
            return await asyncio.wait_for(value_wait[1], timeout)
        finally:
            control.value_waits.remove(value_wait)
            self.drop_control_if_unused(device_name, control_name)

    def fail_waits(self, error: Exception) -> None:
        """
        End every wait with the error, the client is closing.
        """
        for device in self._devices.values():
            futures = list(device.exists_waits)
            for control in device.controls.values():
                futures += control.exists_waits + [future for _, future in control.value_waits]
            for future in futures:
                if not future.done():
                    future.set_exception(error)

    # ------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------

    def add_control_listener(self, device_name: str, control_name: str, event: str, handler: Handler) -> None:
        """
        Register a handler of a control event and queue the calls that describe the current state.
        """
        check_handler_arguments(handler, _HANDLER_ARGUMENTS[event], f"on_{event}")
        control = self.ensure_control(device_name, control_name)
        if not control.exists:
            logger.debug(
                "Control %s/%s does not exist yet, its %s handler waits", device_name, control_name, event
            )
        control.listeners.add_handler(
            event, handler, self._get_initial_calls(device_name, control_name, event)
        )

    def add_device_listener(self, device_name: str, event: str, handler: Handler) -> None:
        """
        Register a handler of an event of the device itself or of any of its controls.
        """
        # Events of the device itself
        if event in _DEVICE_SELF_EVENTS:
            check_handler_arguments(handler, _HANDLER_ARGUMENTS[event], f"on_{event}")
            device = self.ensure_device(device_name)
            initial_calls: list[tuple] = []
            if device.exists and event == "added":
                initial_calls = [(self._make_device(device_name),)]
            elif device.exists and event == "error":
                initial_calls = [(self._make_device(device_name), device.error, None)]
            device.listeners.add_handler(event, handler, initial_calls)
            return

        # Events of every control of the device
        control_event = _CONTROL_EVENT_OF[event]
        check_handler_arguments(handler, _HANDLER_ARGUMENTS[control_event], f"on_{event}")
        device = self.ensure_device(device_name)
        initial_calls = [
            call
            for control_name in self.existing_control_names(device_name)
            for call in self._get_initial_calls(device_name, control_name, control_event)
        ]
        device.listeners.add_handler(event, handler, initial_calls)

    def remove_control_listener(self, device_name: str, control_name: str, handler: Handler) -> None:
        control = self.get_control(device_name, control_name)
        if control is not None:
            control.listeners.remove_handler(handler)
            self.drop_control_if_unused(device_name, control_name)

    def remove_device_listener(self, device_name: str, handler: Handler) -> None:
        device = self._devices.get(device_name)
        if device is not None:
            device.listeners.remove_handler(handler)

    # ------------------------------------------------------------
    # Changes from the broker and from own operations, with their events and waits
    # ------------------------------------------------------------

    def mark_control_added(self, device_name: str, control_name: str, meta: ControlMeta) -> None:
        """
        The control got its meta. A value and an error that came before the meta reach the listeners now.
        """
        # State before the change, for _announce_device_change
        control = self.ensure_control(device_name, control_name)
        state_before = self._get_device_state(device_name)

        # The record gets the meta. A press is an event, a value kept for a pushbutton is a leftover.
        if meta.control_type == ControlType.PUSHBUTTON:
            control.payload = None
        control.meta = meta
        control.generation += 1

        # The waits end, the listeners learn the control appeared and maybe its device too
        for future in control.exists_waits:
            if not future.done():
                future.set_result(None)
        self._announce_device_change(device_name, state_before)
        self._emit_control_event(device_name, control_name, "added")
        self.wake_device_waits(device_name)

        # An error and a value that came before the meta
        if control.error:
            self._emit_control_event(device_name, control_name, "error", control.error, None)
        if control.payload is not None:
            self.set_control_value(device_name, control_name, parse_value(meta, control.payload))

    def set_control_meta(self, device_name: str, control_name: str, meta: ControlMeta) -> None:
        """
        A meta field changed.

        A new type reads the kept payload again by that type. The value did not change in the broker,
        so it is no on_value, and a pushbutton has no value at all. Waits that the new value fits end.
        """
        control = self._devices[device_name].controls[control_name]
        previous = control.meta
        control.meta = meta
        if meta.control_type != previous.control_type:
            if meta.control_type == ControlType.PUSHBUTTON:
                control.payload = None
            control.value = None if control.payload is None else parse_value(meta, control.payload)
            if control.value is not None:
                _wake_value_waits(control)
        self._emit_control_event(device_name, control_name, "meta")

    def mark_control_removed(self, device_name: str, control_name: str) -> None:
        """
        The control is gone. The record is dropped unless listeners or waits hang on it.
        """
        # State before the change, for _announce_device_change
        device = self._devices[device_name]
        control = device.controls[control_name]
        state_before = self._get_device_state(device_name)

        # The record is cleared
        control.meta = None
        control.payload = None
        control.value = None
        control.error = ControlErrorFlags.NONE
        control.generation += 1

        # The listeners of the control and of its device learn it is gone
        if control.listeners:
            logger.debug(
                "Control %s/%s is removed, its handlers wait for it to appear", device_name, control_name
            )
        self._emit_control_event(device_name, control_name, "removed")

        # The record stays only for listeners and waits, the device may go with the control
        if control.is_unused:
            del device.controls[control_name]
        self._announce_device_change(device_name, state_before)

    def drop_control_if_unused(self, device_name: str, control_name: str) -> None:
        """
        Drop the record of a control without meta once nothing is left in it.
        """
        device = self._devices.get(device_name)
        control = device.controls.get(control_name) if device else None
        if control is not None and control.is_unused:
            del device.controls[control_name]

    def set_control_value(self, device_name: str, control_name: str, value: Any) -> None:
        control = self._devices[device_name].controls[control_name]
        if value == control.value or (_is_nan(value) and _is_nan(control.value)):
            return
        previous = control.value
        control.value = value
        if value is None:
            # Cleared without an event, a handler never gets None. Removal comes with the meta.
            return
        self._emit_control_event(device_name, control_name, "value", value, previous)
        _wake_value_waits(control)

    def emit_pushbutton_press(self, device_name: str, control_name: str) -> None:
        """
        A pushbutton was pressed. Every press is an event, the value stays None.
        """
        self._emit_control_event(device_name, control_name, "value", True, None)

    def set_control_error(self, device_name: str, control_name: str, error: ControlErrorFlags) -> None:
        control = self._devices[device_name].controls[control_name]
        if error == control.error:
            return
        previous = control.error
        control.error = error
        if control.exists:
            self._emit_control_event(device_name, control_name, "error", error, previous)

    def emit_command(self, device_name: str, control_name: str, value: Any) -> None:
        """
        A command to an own control, for on_command of the control and on_control_command of the device.
        """
        self._emit_control_event(device_name, control_name, "command", value)

    def has_command_listeners(self, device_name: str, control_name: str) -> bool:
        device = self._devices[device_name]
        return device.controls[control_name].listeners.has_listeners(
            "command"
        ) or device.listeners.has_listeners("control_command")

    def set_device_meta(self, device_name: str, meta: DeviceMeta) -> None:
        # State before the change, for _announce_device_change
        device = self.ensure_device(device_name)
        state_before = self._get_device_state(device_name)

        # The meta. A device the service removed reads as foreign once a meta comes for it.
        device.has_meta = True
        device.title = meta.title
        device.driver = meta.driver
        if device.ownership is Ownership.REMOVED:
            device.ownership = Ownership.FOREIGN

        # The listeners learn what changed, the waits for the device end
        self._announce_device_change(device_name, state_before)
        self.wake_device_waits(device_name)

    def set_device_error(self, device_name: str, error: ControlErrorFlags) -> None:
        device = self.ensure_device(device_name)
        if error == device.error:
            return
        previous = device.error
        device.error = error
        if device.exists and device.listeners.has_listeners("error"):
            device.listeners.emit_event("error", (self._make_device(device_name), error, previous))

    def clear_device_meta(self, device_name: str) -> None:
        """
        The JSON meta of the device is erased, its title and driver go with it.
        """
        # State before the change, for _announce_device_change
        device = self._devices[device_name]
        state_before = self._get_device_state(device_name)

        # The meta goes with its title and driver
        device.has_meta = False
        device.title = TranslatedTitle()
        device.driver = None

        # The listeners learn what changed
        self._announce_device_change(device_name, state_before)

    def make_device_own(self, device_name: str, title: TranslatedTitle, driver: str) -> None:
        """
        The device becomes own. Nothing of the former owner stays, its controls are removed, values and
        errors of controls without meta and the error of the device are cleared.
        """
        # Nothing of the former owner stays
        device = self._devices[device_name]
        for control_name in self.existing_control_names(device_name):
            self.mark_control_removed(device_name, control_name)
        for control_name, control in list(device.controls.items()):
            control.payload = None
            control.value = None
            control.error = ControlErrorFlags.NONE
            self.drop_control_if_unused(device_name, control_name)
        self.set_device_error(device_name, ControlErrorFlags.NONE)

        # State before the change, for _announce_device_change
        state_before = self._get_device_state(device_name)

        # The device becomes own
        device.ownership = Ownership.OWN
        device.has_meta = True
        device.title = title
        device.driver = driver
        device.next_order = 1

        # The listeners learn what changed
        self._announce_device_change(device_name, state_before)

    def mark_device_removed(self, device_name: str) -> None:
        """
        The service removed the own device, it reads like a foreign one from now on. Its controls go with
        on_removed, its error is cleared without on_error, then on_removed of the device tells it is gone.
        """
        device = self._devices[device_name]
        device.ownership = Ownership.REMOVED
        for control_name in reversed(self.existing_control_names(device_name)):
            self.mark_control_removed(device_name, control_name)
        device.error = ControlErrorFlags.NONE
        self.clear_device_meta(device_name)

    def wake_device_waits(self, device_name: str) -> None:
        device = self._devices[device_name]
        if not device.exists:
            return
        for future in device.exists_waits:
            if not future.done():
                future.set_result(None)

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------

    def _get_initial_calls(self, device_name: str, control_name: str, event: str) -> list[tuple]:
        """
        Calls that describe the current state to a handler registered now.

        A known value implies the control exists, the value is cleared when the meta goes.
        """
        control = self.get_control(device_name, control_name)
        if control is None or not control.exists:
            return []
        control_object = self._make_control(device_name, control_name)
        if event == "added":
            return [(control_object,)]
        if event == "value" and control.value is not None:
            return [(control_object, control.value, None)]
        if event == "error":
            return [(control_object, control.error, None)]
        return []

    def _get_device_state(self, device_name: str) -> _DeviceState:
        device = self._devices[device_name]
        return device.exists, device.title, device.driver

    def _announce_device_change(self, device_name: str, state_before: _DeviceState) -> None:
        """
        Tell the listeners of the device itself what changed against the state before a change.

        The device appeared or went, or its title or driver changed while it exists.
        """
        # What changed for the listeners of the device
        existed, title, driver = state_before
        device = self._devices[device_name]
        if device.exists and not existed:
            event = "added"
        elif existed and not device.exists:
            event = "removed"
        elif device.exists and (device.title, device.driver) != (title, driver):
            event = "meta"
        else:
            return

        # The event, and on_error too for a device that appears with an error
        if device.listeners.has_listeners(event):
            device.listeners.emit_event(event, (self._make_device(device_name),))
        if event == "added" and device.error and device.listeners.has_listeners("error"):
            device.listeners.emit_event("error", (self._make_device(device_name), device.error, None))

    def _emit_control_event(self, device_name: str, control_name: str, event: str, *details: Any) -> None:
        """
        Queue an event of a control for its listeners and those of its device. The arguments are built only
        when someone listens.
        """
        device = self._devices[device_name]
        control_listeners = device.controls[control_name].listeners
        device_event = _DEVICE_EVENT_OF[event]
        to_control = control_listeners.has_listeners(event)
        to_device = device.listeners.has_listeners(device_event)
        if not (to_control or to_device):
            return
        args = (self._make_control(device_name, control_name), *details)
        if to_control:
            control_listeners.emit_event(event, args)
        if to_device:
            device.listeners.emit_event(device_event, args)


def _is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


async def _wait_exists(record: Union[DeviceRecord, ControlRecord], timeout: float) -> None:
    if record.exists:
        return
    future = asyncio.get_running_loop().create_future()
    record.exists_waits.append(future)
    try:
        await asyncio.wait_for(future, timeout)
    finally:
        record.exists_waits.remove(future)


def _wake_value_waits(control: ControlRecord) -> None:
    """
    End the waits whose condition the current value of the control fits.
    """
    for predicate, future in list(control.value_waits):
        if future.done():
            continue
        try:
            if predicate(control.value):
                future.set_result(control.value)
        except Exception as error:  # pylint: disable=broad-exception-caught
            future.set_exception(error)
