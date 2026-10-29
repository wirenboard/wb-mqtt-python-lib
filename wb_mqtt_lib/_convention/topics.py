"""
Topics of the Wiren Board convention, how the library builds and parses them.

    /devices/<device>/meta                            JSON meta of the device
    /devices/<device>/meta/error                      error letters of the device itself
    /devices/<device>/controls/<control>              value
    /devices/<device>/controls/<control>/meta         JSON meta of the control
    /devices/<device>/controls/<control>/meta/error   error letters
    /devices/<device>/controls/<control>/on           command
"""

import enum
from dataclasses import dataclass
from typing import Optional

ALL_DEVICE_METAS = "/devices/+/meta"
ALL_CONTROL_METAS = "/devices/+/controls/+/meta"
# Legacy subtopic with the driver name, the signature of a device whose JSON meta has no driver
ALL_LEGACY_DRIVERS = "/devices/+/meta/driver"

# ------------------------------------------------------------
# Parsing
# ------------------------------------------------------------


class TopicKind(enum.Enum):
    """
    What a topic of a device carries.
    """

    DEVICE_META = "device meta"
    DEVICE_ERROR = "device error"
    VALUE = "value"
    META = "meta"
    ERROR = "error"
    COMMAND = "command"


@dataclass(frozen=True)
class ParsedTopic:
    """
    A parsed topic. control is None for the device meta.
    """

    device: str
    control: Optional[str]
    kind: TopicKind


# What a control topic carries by the levels after the control name
_CONTROL_TAILS = {
    (): TopicKind.VALUE,
    ("meta",): TopicKind.META,
    ("meta", "error"): TopicKind.ERROR,
    ("on",): TopicKind.COMMAND,
}


def parse(topic: str) -> Optional[ParsedTopic]:
    """
    Parse a topic of the convention.

    Returns:
        The parsed topic, or None for anything else, for example legacy meta subtopics like /meta/name
        or a topic with an empty device or control name.
    """
    parts = topic.split("/")
    if len(parts) < 4 or parts[0] or parts[1] != "devices" or not parts[2]:
        return None
    if len(parts) == 4:
        return ParsedTopic(parts[2], None, TopicKind.DEVICE_META) if parts[3] == "meta" else None
    if parts[3] == "meta":
        return ParsedTopic(parts[2], None, TopicKind.DEVICE_ERROR) if parts[4:] == ["error"] else None
    if parts[3] != "controls" or not parts[4]:
        return None
    kind = _CONTROL_TAILS.get(tuple(parts[5:]))
    return ParsedTopic(parts[2], parts[4], kind) if kind else None


def device_name(topic: str) -> str:
    """
    Device name from any topic under /devices/<device>/.
    """
    return topic.split("/")[2]


# ------------------------------------------------------------
# Topics and filters of a device
# ------------------------------------------------------------


@dataclass(frozen=True)
class DeviceTopics:
    """
    Prepared publication topics kept with an own device.
    """

    meta: str
    error: str


@dataclass(frozen=True)
class ControlTopics:
    """
    Prepared publication topics kept with an own control.
    """

    value: str
    meta: str
    error: str


def device_meta(device: str) -> str:
    return f"/devices/{device}/meta"


def device_error(device: str) -> str:
    return f"/devices/{device}/meta/error"


def legacy_driver(device: str) -> str:
    return f"/devices/{device}/meta/driver"


def control_value(device: str, control: str) -> str:
    return f"/devices/{device}/controls/{control}"


def control_meta(device: str, control: str) -> str:
    return f"{control_value(device, control)}/meta"


def control_error(device: str, control: str) -> str:
    return f"{control_value(device, control)}/meta/error"


def control_command(device: str, control: str) -> str:
    return f"{control_value(device, control)}/on"


def device_root(device: str) -> str:
    """
    The level every topic of the device lies under.
    """
    return f"/devices/{device}"


def device_tree(device: str) -> str:
    """
    Filter of every topic of the device.
    """
    return f"/devices/{device}/#"


def watch_filters(device: str) -> tuple[str, ...]:
    """
    Filters of everything a reader needs from a device, meta, values and errors without legacy subtopics.
    """
    return (
        device_meta(device),
        control_meta(device, "+"),
        device_error(device),
        control_value(device, "+"),
        control_error(device, "+"),
    )


def command_filter(device: str) -> str:
    return control_command(device, "+")


# ------------------------------------------------------------
# Order of erasure
# ------------------------------------------------------------

# (stage, place inside the stage). Stage 0 is the values, commands and errors of the controls with their
# legacy meta subtopics, stage 1 the control meta, the error of the device and its legacy subtopics,
# stage 2 the device meta and /meta/driver, which name the driver.
_ERASURE_ORDER = {
    TopicKind.VALUE: (0, 0),
    TopicKind.COMMAND: (0, 0),
    TopicKind.ERROR: (0, 1),
    TopicKind.META: (1, 0),
    TopicKind.DEVICE_ERROR: (1, 1),
    TopicKind.DEVICE_META: (2, 0),
}


def erasure_order(topic: str) -> tuple[int, int]:
    """
    Stage of an erasure the topic belongs to and its place inside the stage, a sort key.

    A stage starts after the broker confirmed the one before. By the driver name in the device meta, erased
    last, the next run finds a cleanup that ended halfway. An empty /meta/error after the meta would bring
    an empty card back in an open homeui session. A legacy subtopic goes before the meta it belongs to,
    one of a control with the values, one of the device with the control meta, and /meta/driver goes
    with the device meta.
    """
    parsed = parse(topic)
    if parsed is not None:
        return _ERASURE_ORDER[parsed.kind]
    if _is_legacy_driver(topic):
        return _ERASURE_ORDER[TopicKind.DEVICE_META]
    return _ERASURE_ORDER[TopicKind.ERROR if _is_under_control(topic) else TopicKind.DEVICE_ERROR]


def _is_under_control(topic: str) -> bool:
    parts = topic.split("/")
    return len(parts) > 4 and parts[3] == "controls"


def _is_legacy_driver(topic: str) -> bool:
    parts = topic.split("/")
    return len(parts) == 5 and parts[3:] == ["meta", "driver"]
