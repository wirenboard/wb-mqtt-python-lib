"""
What the broker accepts in a topic, a filter, a QoS and a payload. Nothing here knows the Wiren Board
convention.
"""

from typing import Any, Union

# What a publication takes as its payload
Payload = Union[str, bytes, bytearray, int, float, None]
# mosquitto drops the connection on a topic or a filter with more slashes
MAX_SLASHES = 200
# MQTT limits a topic to 65535 bytes of UTF-8
_MAX_TOPIC_BYTES = 65535

# ------------------------------------------------------------
# Topics and filters
# ------------------------------------------------------------


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
        # + must be a whole level, # a whole last level
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
    refused = sorted({char for char in topic if is_refused_by_broker(char)})
    if refused:
        raise ValueError(f"topic {topic!r} contains characters the broker refuses {refused}")
    if topic.count("/") > MAX_SLASHES:
        raise ValueError(f"topic has more than {MAX_SLASHES} slashes")


def is_refused_by_broker(char: str) -> bool:
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


# ------------------------------------------------------------
# QoS and payloads
# ------------------------------------------------------------


def check_qos(qos: int) -> None:
    """
    Reject a QoS paho cannot pack.

    Raises:
        ValueError: the QoS is not the int 0, 1 or 2. paho fails to pack 1.0 in a subscription.
    """
    if not is_int(qos) or qos not in (0, 1, 2):
        raise ValueError(f"QoS must be 0, 1 or 2, got {qos!r}")


def check_payload(payload: Payload) -> None:
    """
    Reject a payload paho does not take, and bool, which paho would send as «True».

    Raises:
        TypeError: the payload is not str, bytes, bytearray, int, float or None, it is a bool, or a str
            that is not valid UTF-8.
    """
    if not (is_int(payload) or isinstance(payload, (str, bytes, bytearray, float, type(None)))):
        raise TypeError(
            f"payload must be str, bytes, bytearray, int, float or None, got {type(payload).__name__}"
        )
    if isinstance(payload, str) and not is_utf8_text(payload):
        raise TypeError("payload is not valid UTF-8")


# ------------------------------------------------------------
# Types of values
# ------------------------------------------------------------


def is_utf8_text(value: Any) -> bool:
    """
    Whether the value is a string that goes into a topic, a lone surrogate has no UTF-8 form.
    """
    if not isinstance(value, str):
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def is_int(value: Any) -> bool:
    """
    Whether the value is an int. bool is an int in Python, here it is not.
    """
    return isinstance(value, int) and not isinstance(value, bool)
