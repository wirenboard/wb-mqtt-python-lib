"""
Device and Control objects of own and foreign devices.

An object keeps only the client, the name and the device sync of the client, and reads the state
at the moment of the call. Any number of objects for one name see the same state and are equal, so they
are compared with ==, not with is. Only the client builds them.
"""

from typing import TYPE_CHECKING, Any, Mapping, Optional

from .listeners import (
    ChangeHandlerT,
    CommandHandlerT,
    ControlHandlerT,
    DeviceHandlerT,
    Handler,
)
from .values import (
    ControlErrorFlags,
    ControlMeta,
    ControlType,
    TitleLike,
    TranslatedTitle,
)

if TYPE_CHECKING:
    from .client import Client
    from .device_sync import DeviceSync
    from .state import ControlRecord, DeviceRecord

_BUILT_BY_THE_CLIENT = "the objects come from the client, for example from client.get_control"


class Control:  # pylint: disable=too-many-public-methods
    """
    A control of an own or a foreign device by name «device/control».

    The class is for type annotations. Objects come from Client.get_control, Device.controls
    and Device.create_control and as the first argument of a handler, only the client builds them. A handler
    or a wait on a foreign device that the client has not read through get_control or get_device raises
    RuntimeError.

    Raises:
        TypeError: the service calls the constructor itself.
    """

    def __init__(self, device: "Device", name: str, device_sync: Optional["DeviceSync"] = None) -> None:
        if device_sync is None:
            raise TypeError(_BUILT_BY_THE_CLIENT)
        self.device = device
        self.name = name
        self._device_sync = device_sync

    def __repr__(self) -> str:
        return f"<Control {self.full_name} exists={self.exists} value={self.value!r}>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Control) and (self.device, self.name) == (other.device, other.name)

    def __hash__(self) -> int:
        return hash(self.full_name)

    @property
    def full_name(self) -> str:
        """
        Name as «device/control».
        """
        return f"{self.device.name}/{self.name}"

    @property
    def exists(self) -> bool:
        """
        True while the control has meta with a type.
        """
        record = self._record
        return record is not None and record.exists

    @property
    def is_own(self) -> bool:
        """
        True for a control of a device this client created and still publishes.
        """
        return self.device.is_own

    @property
    def meta(self) -> Optional[ControlMeta]:
        """
        Meta of the control with every field, None while it does not exist.
        """
        record = self._record
        return record.meta if record else None

    @property
    def control_type(self) -> Optional[str]:
        meta = self.meta
        return meta.control_type if meta else None

    @property
    def title(self) -> TranslatedTitle:
        meta = self.meta
        return meta.title if meta else TranslatedTitle()

    @property
    def readonly(self) -> bool:
        meta = self.meta
        return bool(meta and meta.readonly)

    @property
    def units(self) -> Optional[str]:
        meta = self.meta
        return meta.units if meta else None

    @property
    def value(self) -> Any:
        """
        Current value by type.

        switch, alarm and wo-switch give bool, value, range and the deprecated number types like temperature
        a number, local_time and w1-id an int, rgb three ints, text and every other type a str. A foreign
        payload that does not parse by the type stays a str. A pushbutton has no value, it is always None.
        None also while the control does not exist or its value has not arrived yet.
        """
        record = self._record
        return record.value if record else None

    @property
    def error(self) -> ControlErrorFlags:
        record = self._record
        return record.error if record else ControlErrorFlags.NONE

    def on_added(self, handler: ControlHandlerT) -> ControlHandlerT:
        """
        handler(control) when the control appears. Called at once if it exists already.
        """
        return self._listen("added", handler)

    def on_removed(self, handler: ControlHandlerT) -> ControlHandlerT:
        """
        handler(control) when the meta of the control is erased.
        """
        return self._listen("removed", handler)

    def on_value(self, handler: ChangeHandlerT) -> ChangeHandlerT:
        """
        handler(control, value, previous) on every change in the broker, for pushbutton on every press.

        Called at once with previous=None when the value is known already. A new type of a foreign control
        reads the kept payload again and changes value without this event, only on_meta comes.
        """
        return self._listen("value", handler)

    def on_error(self, handler: ChangeHandlerT) -> ChangeHandlerT:
        """
        handler(control, error, previous) on every change.

        Called at once with previous=None if the control exists.
        """
        return self._listen("error", handler)

    def on_meta(self, handler: ControlHandlerT) -> ControlHandlerT:
        """
        handler(control) when a meta field such as the title or readonly changes.
        """
        return self._listen("meta", handler)

    def on_command(self, handler: CommandHandlerT) -> CommandHandlerT:
        """
        handler(control, value) for a command from /on. Own controls only, the control may not exist yet,
        a removed one waits until the service creates it again.

        The handler decides what to publish, a command without a handler is dropped.

        Raises:
            NotOwnError: the control is foreign.
        """
        return self._listen("command", handler)

    def remove_handler(self, handler: Handler) -> None:
        """
        Take a handler off every on_* of this control. Other controls and the device keep theirs.

        Events the handler has not started are dropped. A call that runs now is cancelled at its next await,
        unless remove_handler is called from that handler itself.
        """
        self._device_sync.state.remove_control_listener(self.device.name, self.name, handler)

    async def wait_value(self, expected: Any, timeout: float) -> Any:
        """
        Wait until the value equals expected or, when expected is a function, until it returns True.

        Waits outside the listener queues, so a handler may call it. An exception of the function reaches
        the caller. None never fits, an erased value or a control that is gone does not end the wait.

        Args:
            expected: the value, or a function that takes the value and returns True when it fits.
            timeout: seconds to wait.

        Returns:
            The value that fits.

        Raises:
            TimeoutError: the value did not come in time.
            RuntimeError: the device is foreign and the client has not read it.
            ClientClosedError: the client is closed or closes while waiting.
        """
        return await self._device_sync.wait_value(self.device.name, self.name, expected, timeout)

    async def set_value(self, value: Any) -> None:
        """
        Own control: value takes the new value at once, the payload goes out when it changed, a pushbutton
        press every time. precision rounds only the payload.

        Foreign control: send a command to /on, the value changes when the driver publishes it.

        switch, alarm and wo-switch take bool, not 0 or 1, a pushbutton only True, value and range
        a finite number, local_time and w1-id an int, rgb three ints from 0 to 255, kept as a tuple, text
        anything but None whose str() is valid in UTF-8, kept as str. None clears the value of an own control
        other than a pushbutton, the retained message is erased, value becomes None and no event comes.

        A change of an own control returns once the connection took the message, without waiting for
        the broker to confirm it. It waits for the broker when 20 messages of the client already wait
        for confirmation or messages wait in the queue, for example an erasure after remove() or the replay
        after reconnect, so a loop cannot run ahead of the broker. A command and a press return once
        the broker confirmed them.

        Raises:
            TypeError: the value does not fit the control type.
            ControlMissingError: the control does not exist.
            BrokerDisconnectedError: a command or a pushbutton press while the broker link is down,
                or the link is lost or the broker does not confirm it in 5 s.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_sync.set_value(self.device.name, self.name, value)

    async def set_error(self, error: ControlErrorFlags) -> None:
        """
        Own control: set or clear the error. The library never clears it by itself. Returns without waiting
        for the broker, as set_value of an own control.

        Raises:
            TypeError: the error is not ControlErrorFlags.
            NotOwnError: the control is foreign.
            ControlMissingError: the control or its device does not exist.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_sync.set_error(self.device.name, self.name, error)

    async def update_meta(self, **fields: Any) -> None:
        """
        Own control: change meta fields on the fly, for example title, units or readonly.

        A field set back to None leaves the meta, an open homeui page keeps the old one until it is reloaded.
        readonly and hidden take only True or False. With a link it returns once the broker confirmed
        the meta, or after 5 s without confirmation, then the meta goes again.

        Raises:
            TypeError: an unknown field, or a field of the wrong type as in Device.create_control.
            ValueError: precision is not positive, or minimum or maximum is not finite.
            NotOwnError: the control is foreign.
            ControlMissingError: the control or its device does not exist.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_sync.update_control_meta(self.device.name, self.name, **fields)

    async def remove(self) -> None:
        """
        Own control: erase everything the client published under the control, raw legacy subtopics too.
        Listeners stay and see the control again if it is created again.

        Nothing happens when the control does not exist, for example after remove() of its device.

        Raises:
            NotOwnError: the control is foreign.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_sync.remove_control(self.device.name, self.name)

    def _listen(self, kind: str, handler: Any) -> Any:
        self._device_sync.add_control_listener(self.device.name, self.name, kind, handler)
        return handler

    @property
    def _record(self) -> Optional["ControlRecord"]:
        return self._device_sync.state.control(self.device.name, self.name)


class Device:  # pylint: disable=too-many-public-methods
    """
    An own or a foreign device by name. Exists while it has JSON meta or at least one control.

    The class is for type annotations. Objects come from Client.create_device and Client.get_device
    and as an argument of a handler, only the client builds them. A handler on a foreign device that
    the client has not read through get_device or get_control raises RuntimeError.

    Raises:
        TypeError: the service calls the constructor itself.
    """

    def __init__(self, client: "Client", name: str, device_sync: Optional["DeviceSync"] = None) -> None:
        if device_sync is None:
            raise TypeError(_BUILT_BY_THE_CLIENT)
        self._client = client
        self.name = name
        self._device_sync = device_sync

    def __repr__(self) -> str:
        return f"<Device {self.name} own={self.is_own} exists={self.exists}>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Device) and self._client is other._client and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    @property
    def exists(self) -> bool:
        record = self._record
        return record is not None and record.exists

    @property
    def is_own(self) -> bool:
        """
        True for a device this client created and still publishes.
        """
        return self._device_sync.state.is_own(self.name)

    @property
    def title(self) -> TranslatedTitle:
        record = self._record
        return record.title if record else TranslatedTitle()

    @property
    def driver(self) -> Optional[str]:
        record = self._record
        return record.driver if record else None

    @property
    def error(self) -> ControlErrorFlags:
        """
        Error of the device itself, NONE without one. homeui marks the card red for any error.
        """
        record = self._record
        return record.error if record else ControlErrorFlags.NONE

    @property
    def controls(self) -> dict[str, Control]:
        """
        Controls that exist now, by name.
        """
        return {
            name: Control(self, name, self._device_sync)
            for name in self._device_sync.state.existing_control_names(self.name)
        }

    def on_added(self, handler: DeviceHandlerT) -> DeviceHandlerT:
        """
        handler(device) when the device appears, with its JSON meta or its first control. Called at once if
        it exists already.
        """
        return self._listen("added", handler)

    def on_removed(self, handler: DeviceHandlerT) -> DeviceHandlerT:
        """
        handler(device) when the device is gone, its JSON meta and every control erased.
        """
        return self._listen("removed", handler)

    def on_meta(self, handler: DeviceHandlerT) -> DeviceHandlerT:
        """
        handler(device) when the title or the driver changes while the device exists.
        """
        return self._listen("meta", handler)

    def on_error(self, handler: ChangeHandlerT) -> ChangeHandlerT:
        """
        handler(device, error, previous) when the error of the device itself changes.

        Called at once with previous=None if the device exists.
        """
        return self._listen("error", handler)

    def on_control_added(self, handler: ControlHandlerT) -> ControlHandlerT:
        """
        handler(control) when a control of the device appears. Called at once for existing controls.
        """
        return self._listen("control_added", handler)

    def on_control_removed(self, handler: ControlHandlerT) -> ControlHandlerT:
        """
        handler(control) when a control of the device is removed.
        """
        return self._listen("control_removed", handler)

    def on_control_value(self, handler: ChangeHandlerT) -> ChangeHandlerT:
        """
        handler(control, value, previous) for any control of the device. Called at once for known values.
        """
        return self._listen("control_value", handler)

    def on_control_error(self, handler: ChangeHandlerT) -> ChangeHandlerT:
        """
        handler(control, error, previous) for any control of the device.

        Called at once for existing controls.
        """
        return self._listen("control_error", handler)

    def on_control_meta(self, handler: ControlHandlerT) -> ControlHandlerT:
        """
        handler(control) when meta of any control of the device changes.
        """
        return self._listen("control_meta", handler)

    def on_control_command(self, handler: CommandHandlerT) -> CommandHandlerT:
        """
        handler(control, value) for a command from /on to any control of the own device.

        The commands of all controls share one queue, so they reach the handler in the order they came.
        A removed device keeps the handler until the service creates it again. control.remove_handler does
        not take it off, device.remove_handler does.

        Raises:
            NotOwnError: the device is foreign.
        """
        return self._listen("control_command", handler)

    def remove_handler(self, handler: Handler) -> None:
        """
        Take a handler off every on_* of this device, its own events and on_control_*. The same handler
        registered on a control of the device stays there.

        Events the handler has not started are dropped. A call that runs now is cancelled at its next await,
        unless remove_handler is called from that handler itself.
        """
        self._device_sync.state.remove_device_listener(self.name, handler)

    async def create_control(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        name: str,
        control_type: ControlType,
        title: TitleLike = None,
        units: Optional[str] = None,
        precision: Optional[float] = None,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        order: Optional[int] = None,
        readonly: bool = False,
        hidden: bool = False,
        enum: Optional[Mapping[str, TitleLike]] = None,
        value: Any = None,
    ) -> Control:
        """
        Own device: publish a control. Nothing is published unless the whole declaration is valid.

        Without a value the web interface shows a placeholder until the first set_value, for example 0
        for a number and off for a switch.

        Args:
            name: control name.
            control_type: one of the current types of the convention, ControlType.SWITCH and so on.
            title: a string is English, or {"en": ..., "ru": ...}.
            units: units of value and range.
            precision: step of a number, a positive number. The MQTT payload keeps as many decimals as
                precision has, as in wb-rules, control.value and events keep the accepted number. homeui
                uses it as the step of the input.
            minimum: lower limit of range and value.
            maximum: upper limit of range and value.
            order: position on the card, by creation when not given.
            readonly: True when the control takes no commands.
            hidden: True to hide the control in the web interface.
            enum: {value: title} for a value with names.
            value: initial value, see Control.set_value for the types. A pushbutton takes none.

        Raises:
            ValueError: the name, the type, precision, minimum or maximum is invalid, or the control
                exists already.
            TypeError: the value does not fit the type, or a field has the wrong type, for example units
                that is not a string, order that is not an int, or a title or enum that is not valid.
            NotOwnError: the device is foreign.
            ControlMissingError: the device is removed.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_sync.create_control(
            self.name,
            name,
            control_type,
            value=value,
            title=title,
            units=units,
            precision=precision,
            minimum=minimum,
            maximum=maximum,
            order=order,
            readonly=readonly,
            hidden=hidden,
            enum=enum,
        )
        return Control(self, name, self._device_sync)

    async def set_error(self, error: ControlErrorFlags) -> None:
        """
        Own device: set or clear the error of the device itself. The library never clears it by itself.
        Returns without waiting for the broker, as set_value of an own control.

        Raises:
            TypeError: the error is not ControlErrorFlags.
            NotOwnError: the device is foreign.
            ControlMissingError: the device is removed.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_sync.set_device_error(self.name, error)

    async def update_meta(self, title: TitleLike) -> None:
        """
        Own device: change the title. With a link it returns once the broker confirmed the meta, or after
        5 s without confirmation, then the meta goes again.

        Raises:
            TypeError: the title is not a string or a mapping.
            NotOwnError: the device is foreign.
            ControlMissingError: the device is removed.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_sync.update_device_meta(self.name, title)

    async def remove(self) -> None:
        """
        Own device: erase everything the client published under the device, raw legacy subtopics too.
        A second call does nothing.

        The erasure goes in three stages, each after the broker confirmed the one before: the values,
        errors and legacy meta subtopics of the controls, then their meta, the error of the device and its
        legacy subtopics, then the device meta and /meta/driver. If the service dies halfway, the meta with
        the driver name stays and remove_driver_devices finds it.

        Raises:
            NotOwnError: the device is foreign.
            ClientClosedError: the client is closed or closes while waiting.
        """
        await self._device_sync.remove_device(self.name)

    def _listen(self, kind: str, handler: Any) -> Any:
        self._device_sync.add_device_listener(self.name, kind, handler)
        return handler

    @property
    def _record(self) -> Optional["DeviceRecord"]:
        return self._device_sync.state.device(self.name)
