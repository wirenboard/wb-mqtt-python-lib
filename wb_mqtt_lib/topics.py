"""
Topics: what the broker accepts in a topic and a filter, and the topics of the Wiren Board convention,
how the library builds and parses them.

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
# mosquitto drops the connection on a topic or a filter with more slashes
MAX_SLASHES = 200
# MQTT limits a topic to 65535 bytes of UTF-8
_MAX_TOPIC_BYTES = 65535


class CheckedTopic(str):
    """
    An immutable publication topic checked once before it enters the own device state.
    """

    __slots__ = ()

    def __new__(cls, topic: str) -> "CheckedTopic":
        check_topic(topic)
        return super().__new__(cls, topic)


def check_topic(topic: str) -> None:
    """
    Reject a topic the client or the broker would refuse.

    Mirrored and refused later, it would break the replay after every reconnect, and mosquitto drops
    the connection on such a topic.

    Raises:
        ValueError: the topic is empty, has a wildcard or a character the broker refuses, has more than
            200 slashes or is not valid UTF-8 of at most 65535 bytes.
    """
    if not isinstance(topic, str) or not topic or "+" in topic or "#" in topic:
        raise ValueError(f"invalid topic {topic!r}")
    _check_topic_text(topic)


def check_filter(topic_filter: str) -> None:
    """
    Reject a filter the broker would refuse.

    Registered without a link, it would come back on every resubscription after reconnect.

    Raises:
        ValueError: the filter is empty, has a character the broker refuses, a wildcard that is not a whole
            level or # not at the end, has more than 200 slashes, or it is a shared subscription $share/....
    """
    if not isinstance(topic_filter, str) or not topic_filter:
        raise ValueError(f"invalid topic filter {topic_filter!r}")
    if topic_filter.startswith("$share/"):
        raise ValueError(f"shared subscriptions are not supported, got {topic_filter!r}")
    levels = topic_filter.split("/")
    for index, level in enumerate(levels):
        if ("#" in level and (level != "#" or index != len(levels) - 1)) or ("+" in level and level != "+"):
            raise ValueError(f"invalid topic filter {topic_filter!r}")
    _check_topic_text(topic_filter)


def _check_topic_text(topic: str) -> None:
    try:
        encoded = topic.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"topic {topic!r} is not valid UTF-8") from None
    if len(encoded) > _MAX_TOPIC_BYTES:
        raise ValueError(f"topic is longer than {_MAX_TOPIC_BYTES} bytes")
    refused = sorted({char for char in topic if broker_refuses(char)})
    if refused:
        raise ValueError(f"topic {topic!r} contains characters the broker refuses {refused}")
    if topic.count("/") > MAX_SLASHES:
        raise ValueError(f"topic has more than {MAX_SLASHES} slashes")


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


def broker_refuses(char: str) -> bool:
    """
    Whether mosquitto drops the connection on a topic or a login with this character.

    These are NUL and other control characters, surrogates and Unicode non-characters, as
    mosquitto_validate_utf8 checks them.
    """
    code = ord(char)
    return (
        code <= 0x1F
        or 0x7F <= code <= 0x9F
        or 0xD800 <= code <= 0xDFFF
        or 0xFDD0 <= code <= 0xFDEF
        or (code & 0xFFFF) in (0xFFFE, 0xFFFF)
    )


def device_name(topic: str) -> str:
    """
    Device name from any topic under /devices/<device>/.
    """
    return topic.split("/")[2]


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
    return f"/devices/{device}/#"


def watch_filters(device: str) -> tuple[str, ...]:
    """
    Filters of everything a reader needs from a device: meta, values and errors, without legacy subtopics.
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
