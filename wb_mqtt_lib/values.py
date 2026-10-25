"""
Values, titles, errors and meta in the form the Wiren Board convention puts them into MQTT.

What the service passes in is checked strictly. Meta of foreign devices comes from any client
on the controller, so it is parsed leniently: a broken field is dropped, a broken meta is ignored as a whole.
"""

import json
import math
from dataclasses import dataclass, field, fields
from decimal import Decimal
from enum import Flag, StrEnum, auto
from types import MappingProxyType
from typing import Any, Mapping, Optional, Union

from .topics import broker_refuses


class ControlType(StrEnum):
    """
    Current control types of the convention, the ones an own control is created with.

    A member equals its string, so it compares with the type of a foreign control as is. Deprecated types
    of foreign controls (temperature, voltage and so on) stay strings, their values are read as numbers
    like those of value. Unknown types and their values stay strings.
    """

    SWITCH = "switch"
    WO_SWITCH = "wo-switch"
    ALARM = "alarm"
    PUSHBUTTON = "pushbutton"
    RANGE = "range"
    RGB = "rgb"
    TEXT = "text"
    LOCAL_TIME = "local_time"
    W1_ID = "w1-id"
    VALUE = "value"


_BOOLEAN_TYPES = frozenset({ControlType.SWITCH, ControlType.ALARM, ControlType.WO_SWITCH})
_NUMBER_TYPES = frozenset({ControlType.VALUE, ControlType.RANGE})
# Deprecated types of the convention derived from value, as wbgong lists them. wb-rules reads them as numbers.
_DEPRECATED_NUMBER_TYPES = frozenset(
    {
        "temperature",
        "rel_humidity",
        "atmospheric_pressure",
        "rainfall",
        "wind_speed",
        "power",
        "power_consumption",
        "voltage",
        "water_flow",
        "water_consumption",
        "resistance",
        "concentration",
        "pressure",
        "lux",
        "sound_level",
        "heat_power",
        "heat_energy",
        "current",
    }
)
_READ_AS_NUMBER = _NUMBER_TYPES | _DEPRECATED_NUMBER_TYPES
# Integers by the convention: seconds of local time, a 64-bit 1-Wire address
_INTEGER_TYPES = frozenset({ControlType.LOCAL_TIME, ControlType.W1_ID})
_FORBIDDEN_NAME_CHARS = frozenset("/+#")


def check_name(name: str, kind: str) -> None:
    """
    Reject a device, control or driver name that would break the topic structure or the connection.

    Raises:
        ValueError: the name is empty or contains /, +, #, a control character or another character
            the broker refuses.
    """
    if not isinstance(name, str) or not name:
        raise ValueError(f"{kind} name must be a non-empty string")
    forbidden = {char for char in name if char in _FORBIDDEN_NAME_CHARS or broker_refuses(char)}
    if forbidden:
        raise ValueError(f"{kind} name {name!r} contains forbidden characters {sorted(forbidden)}")


@dataclass(frozen=True)
class TranslatedTitle:
    """
    Title in English and Russian. A title given as one string is English.
    """

    en: Optional[str] = None
    ru: Optional[str] = None

    @classmethod
    def parse(cls, title: "TitleLike") -> "TranslatedTitle":
        """
        Build a title from what the service passes.

        Args:
            title: a string, a {"en": ..., "ru": ...} mapping, a TranslatedTitle or None.

        Raises:
            TypeError: anything else, or a text that is not a string valid in UTF-8.
        """
        if title is None:
            return cls()
        if isinstance(title, TranslatedTitle):
            texts = (title.en, title.ru)
        elif isinstance(title, str):
            texts = (title, None)
        elif isinstance(title, Mapping):
            texts = (title.get("en"), title.get("ru"))
        else:
            raise TypeError(f"unsupported title {title!r}")
        if not all(text is None or is_utf8_text(text) for text in texts):
            raise TypeError(f"title texts must be strings, got {title!r}")
        return cls(*texts)

    @classmethod
    def from_meta(cls, title: Any) -> "TranslatedTitle":
        """
        Title from foreign meta. Anything that is not a string or a mapping of strings is an empty title.
        """
        if isinstance(title, str):
            return cls(en=title)
        if isinstance(title, Mapping):
            return cls(en=_str_or_none(title.get("en")), ru=_str_or_none(title.get("ru")))
        return cls()

    def is_empty(self) -> bool:
        return not self.en and not self.ru

    def get(self, lang: str, default: Optional[str] = None) -> Optional[str]:
        """
        Text in the language, else in the other language, else the default, the way homeui shows a title.

        Args:
            lang: "en" or "ru".
            default: what to return when there is no text at all, for example the control name.
        """
        first, second = (self.ru, self.en) if lang == "ru" else (self.en, self.ru)
        return first or second or default

    def to_dict(self) -> dict[str, str]:
        """
        Languages that have text, as the convention writes them into JSON meta.
        """
        return {lang: text for lang, text in (("en", self.en), ("ru", self.ru)) if text}


TitleLike = Union[None, str, Mapping[str, str], TranslatedTitle]


class ControlErrorFlags(Flag):
    """
    Error of a control: r read, w write, p poll. NONE means no error.
    """

    NONE = 0
    READ = auto()
    WRITE = auto()
    POLL = auto()

    def to_mqtt(self) -> str:
        """
        Letters as the convention writes them, empty for no error.
        """
        return "".join(char for flag, char in _ERROR_CHARS if self & flag)

    @classmethod
    def from_mqtt(cls, text: str) -> "ControlErrorFlags":
        """
        Flags from the letters in a topic. Letters other than r, w and p are ignored.
        """
        error = cls.NONE
        for flag, char in _ERROR_CHARS:
            if char in text:
                error |= flag
        return error


_ERROR_CHARS = ((ControlErrorFlags.READ, "r"), (ControlErrorFlags.WRITE, "w"), (ControlErrorFlags.POLL, "p"))


def _parse_enum(enum: Optional[Mapping[str, TitleLike]]) -> Optional[Mapping[str, TranslatedTitle]]:
    """
    Enum of a control as {value: title} from what the service passes. A value without a title shows as itself.

    The result is read-only, the meta changes only through update_meta.

    Raises:
        TypeError: enum is not a mapping, a value is not valid in UTF-8 or a title is not valid.
    """
    if enum is None:
        return None
    if not isinstance(enum, Mapping):
        raise TypeError(f"enum must be a mapping {{value: title}}, got {enum!r}")
    parsed = {}
    for value, title in enum.items():
        key = str(value)
        if not is_utf8_text(key):
            raise TypeError(f"enum value {key!r} is not valid in UTF-8")
        text = TranslatedTitle.parse(title)
        parsed[key] = TranslatedTitle(en=key) if text.is_empty() else text
    return MappingProxyType(parsed)


@dataclass(frozen=True)
class ControlMeta:  # pylint: disable=too-many-instance-attributes
    """
    Everything the JSON meta of a control carries.
    """

    control_type: str
    title: TranslatedTitle = field(default_factory=TranslatedTitle)
    units: Optional[str] = None
    precision: Optional[float] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    order: Optional[int] = None
    readonly: bool = False
    hidden: bool = False
    # Read-only, a changed copy goes through update_meta
    enum: Optional[Mapping[str, TranslatedTitle]] = None

    def to_json(self) -> str:
        """
        JSON meta as the service publishes it.

        readonly and hidden always go out, homeui keeps a field that a newer meta leaves out.
        """
        meta: dict[str, Any] = {"type": self.control_type, "readonly": self.readonly, "hidden": self.hidden}
        if not self.title.is_empty():
            meta["title"] = self.title.to_dict()
        optional = (
            ("units", self.units),
            ("precision", self.precision),
            ("min", self.minimum),
            ("max", self.maximum),
            ("order", self.order),
        )
        meta.update((key, value) for key, value in optional if value is not None)
        if self.enum:
            meta["enum"] = {value: title.to_dict() for value, title in self.enum.items()}
        return json.dumps(meta, ensure_ascii=False)


# The type of an own control is given apart from its meta fields
_META_FIELDS = frozenset(item.name for item in fields(ControlMeta)) - {"control_type"}


def parse_meta_fields(meta_fields: Mapping[str, Any]) -> dict[str, Any]:
    """
    Fields of an own control meta from what the service passes, checked before anything changes.

    title and enum come parsed, the other fields as they are. A field that is None leaves the meta,
    readonly and hidden always stay in it.

    Raises:
        TypeError: a field is unknown, units is not a string, order is not an int, readonly or hidden
            is not a bool, precision, minimum or maximum is not a number, or title or enum is not valid.
        ValueError: precision is not positive, or precision, minimum or maximum is not finite.
    """
    unknown = set(meta_fields) - _META_FIELDS
    if unknown:
        raise TypeError(f"unknown meta fields {sorted(unknown)}")
    for name in ("precision", "minimum", "maximum"):
        number = meta_fields.get(name)
        if number is None:
            continue
        if not _is_number(number):
            raise TypeError(f"{name} must be a number, got {number!r}")
        if not _is_finite(number):
            # JSON has no infinity or NaN
            raise ValueError(f"{name} must be a finite number, got {number!r}")
        if name == "precision" and number <= 0:
            raise ValueError(f"precision must be a positive number, got {number!r}")
    units = meta_fields.get("units")
    if units is not None and not is_utf8_text(units):
        raise TypeError(f"units must be a string, got {units!r}")
    order = meta_fields.get("order")
    if order is not None and not is_int(order):
        raise TypeError(f"order must be an int, got {order!r}")
    for flag in ("readonly", "hidden"):
        if flag in meta_fields and not isinstance(meta_fields[flag], bool):
            raise TypeError(f"{flag} must be True or False, got {meta_fields[flag]!r}")
    parsed = dict(meta_fields)
    if "title" in parsed:
        parsed["title"] = TranslatedTitle.parse(parsed["title"])
    if "enum" in parsed:
        parsed["enum"] = _parse_enum(parsed["enum"])
    return parsed


def parse_control_meta(payload: str) -> tuple[Optional[ControlMeta], Optional[ControlErrorFlags]]:
    """
    Meta and the error key from the JSON meta of a foreign control.

    Returns:
        (meta, error). meta is None when the payload is not a JSON object with a string type, the control
        is then incomplete. error is None when there is no error key.
    """
    data = _parse_json_object(payload)
    if data is None or not isinstance(data.get("type"), str):
        return None, None
    enum = data.get("enum")
    meta = ControlMeta(
        control_type=data["type"],
        title=TranslatedTitle.from_meta(data.get("title")),
        units=_str_or_none(data.get("units")),
        precision=_precision_or_none(data.get("precision")),
        minimum=_number_or_none(data.get("min")),
        maximum=_number_or_none(data.get("max")),
        order=_int_or_none(data.get("order")),
        readonly=data.get("readonly") in (True, 1),
        hidden=data.get("hidden") in (True, 1),
        enum=(
            MappingProxyType({str(value): TranslatedTitle.from_meta(title) for value, title in enum.items()})
            if isinstance(enum, Mapping)
            else None
        ),
    )
    error = data.get("error")
    return meta, ControlErrorFlags.from_mqtt(error) if isinstance(error, str) else None


@dataclass(frozen=True)
class DeviceMeta:
    """
    What the JSON meta of a device carries.
    """

    title: TranslatedTitle = field(default_factory=TranslatedTitle)
    driver: Optional[str] = None

    def to_json(self) -> str:
        meta: dict[str, Any] = {"driver": self.driver}
        if not self.title.is_empty():
            meta["title"] = self.title.to_dict()
        return json.dumps(meta, ensure_ascii=False)


@dataclass(frozen=True)
class ControlSnapshot:
    """
    A control as the broker kept it at the moment of the read. It does not change afterwards.
    """

    meta: ControlMeta
    value: Any = None
    error: ControlErrorFlags = ControlErrorFlags.NONE


@dataclass(frozen=True)
class DeviceSnapshot:
    """
    A device as the broker kept it at the moment of the read. It does not change afterwards.

    Controls without meta are left out, a pushbutton has no value.
    """

    name: str
    title: TranslatedTitle
    driver: Optional[str]
    controls: dict[str, ControlSnapshot]
    error: ControlErrorFlags = ControlErrorFlags.NONE


def parse_device_meta(payload: str) -> Optional[DeviceMeta]:
    """
    JSON meta of a foreign device, None when the payload is not a JSON object.
    """
    data = _parse_json_object(payload)
    if data is None:
        return None
    return DeviceMeta(TranslatedTitle.from_meta(data.get("title")), _str_or_none(data.get("driver")))


def to_payload_and_value(meta: ControlMeta, value: Any) -> tuple[str, Any]:
    """
    Value of the service turned into the string for the topic and the value the own control keeps.

    switch, alarm and wo-switch take bool, pushbutton takes True. value, range and the deprecated number types
    of foreign controls, temperature for example, take a finite number, precision rounds only the string.
    local_time and w1-id take an int. rgb takes three ints from 0 to 255 and keeps them as a tuple. text
    takes anything but None whose str() is valid in UTF-8 and keeps the str.

    Raises:
        TypeError: the value does not fit the control type.
    """
    control_type = meta.control_type
    if control_type in _BOOLEAN_TYPES:
        if not isinstance(value, bool):
            raise TypeError(f"{control_type} control takes True or False, got {value!r}")
        return ("1" if value else "0"), value
    if control_type == ControlType.PUSHBUTTON:
        if value is not True:
            raise TypeError("a pushbutton press is set_value(True)")
        return "1", True
    if control_type in _READ_AS_NUMBER:
        if not _is_number(value) or not _is_finite(value):
            raise TypeError(f"{control_type} control takes a finite number, got {value!r}")
        return _format_number(value, meta.precision), value
    if control_type in _INTEGER_TYPES:
        if not is_int(value):
            raise TypeError(f"{control_type} control takes an int, got {value!r}")
        return str(value), value
    if control_type == ControlType.RGB:
        if (
            not isinstance(value, (tuple, list))
            or len(value) != 3
            or not all(is_int(part) and 0 <= part <= 255 for part in value)
        ):
            raise TypeError(f"rgb control takes three ints from 0 to 255, got {value!r}")
        normalized = tuple(value)
        return ";".join(str(part) for part in normalized), normalized
    if value is None:
        raise TypeError(f"{control_type} control takes a value, got None")
    text = str(value)
    if not is_utf8_text(text):
        raise TypeError(f"{control_type} control takes text valid in UTF-8, got {text!r}")
    return text, text


def from_payload(meta: ControlMeta, payload: str) -> Any:
    """
    String from a topic turned into a value by the control type, as control.value has it.

    A deprecated type derived from value, temperature or voltage for example, gives a number too.
    A string that does not parse stays a string, and so does the value of an unknown type.
    """
    control_type = meta.control_type
    try:
        if control_type in _BOOLEAN_TYPES:
            return float(payload) != 0
        if control_type in _READ_AS_NUMBER:
            return _parse_number(payload)
        if control_type in _INTEGER_TYPES:
            return int(payload)
        if control_type == ControlType.RGB:
            red, green, blue = (int(part) for part in payload.split(";"))
            return (red, green, blue)
    except ValueError:
        return payload
    return payload


def _format_number(value: Union[int, float], precision: Optional[float]) -> str:
    if not precision:
        if isinstance(value, int):
            return str(value)
        # As wb-rules does: the shortest digits, without an exponent and without a trailing .0
        text = format(Decimal(repr(value)).normalize(), "f")
        return "0" if text == "-0" else text
    # As wb-rules does: as many decimals as precision has, 0.25 gives two and 2.5 gives one,
    # without trailing zeros
    decimals = max(0, -Decimal(str(precision)).normalize().as_tuple().exponent)
    text = f"{value:.{decimals}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def _parse_number(payload: str) -> Union[int, float]:
    try:
        return int(payload)
    except ValueError:
        return float(payload)


def _parse_json_object(payload: str) -> Optional[dict]:
    try:
        data = json.loads(payload)
    except (ValueError, RecursionError):
        return None
    return data if isinstance(data, dict) else None


def _str_or_none(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


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


def _number_or_none(value: Any) -> Optional[float]:
    return value if _is_number(value) and _is_finite(value) else None


def _precision_or_none(value: Any) -> Optional[float]:
    number = _number_or_none(value)
    return number if number is not None and number > 0 else None


def _int_or_none(value: Any) -> Optional[int]:
    return value if is_int(value) else None


def is_int(value: Any) -> bool:
    """
    Whether the value is an int. bool is an int in Python, here it is not.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return is_int(value) or isinstance(value, float)


def _is_finite(number: Union[int, float]) -> bool:
    try:
        return math.isfinite(number)
    except OverflowError:
        # An int too large for a float
        return False
