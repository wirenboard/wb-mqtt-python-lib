"""
The broker connection over aiomqtt and paho. Everything the library needs from them goes through here.

The publication queue and the subscriptions rely on three properties of aiomqtt over paho:

1. Order. publish, subscribe and unsubscribe hand the packet to paho before their first await. Packets go
   to the broker in the order of the calls, also when the caller awaits the answer later in its own task.
2. A lost answer and a broken link differ. aiomqtt ends a wait for PUBACK that runs out with a plain
   MqttError while paho still takes packets, and a call paho refuses with MqttCodeError. Only the first
   is worth sending again on the same connection. Here they become AckTimeout and LinkLost.
3. Window. paho keeps at most 20 QoS 1 publications without PUBACK and queues the rest. A wait for PUBACK
   starts when the packet is handed to paho, so more than SEND_WINDOW at once would run out in that queue.

aiomqtt does not end the waits for PUBACK when the connection drops, they end by ACK_TIMEOUT_S.
"""

import logging
from typing import Any, Callable, Optional

import aiomqtt
from paho.mqtt.matcher import MQTTMatcher
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions

from .._logs import get_logger
from .broker_url import BrokerAddress
from .checks import Payload

# Matches topics against the filters of the subscriptions
TopicMatcher = MQTTMatcher

# A dead link goes unnoticed for one or two keepalives
KEEPALIVE_S = 15
# Deadline for a broker acknowledgement
ACK_TIMEOUT_S = 5.0
# Publications waiting for PUBACK at once, the limit of paho, see the module docstring
SEND_WINDOW = 20
# Every handler publishes on its own, a burst easily has dozens of publications waiting for the broker,
# and aiomqtt warns about each one above this number
PENDING_CALLS_WARNING = 10_000
# MQTT reason codes of a rejected login, a bad user name or password and not authorized
_LOGIN_REJECTED_CODES = (134, 135)

# Called for every message from the broker with its topic, payload, retain flag as published, QoS and
# the numbers of the subscriptions it came for, None when the broker sent none
PacketHandler = Callable[[str, bytes, bool, int, Optional[list[int]]], None]

_logger = get_logger(f"{__name__}.aiomqtt")


class _LinkNoiseFilter(logging.Filter):  # pylint: disable=too-few-public-methods
    """
    paho logs a broken socket as ERROR. For the service it is a lost link, and the client logs that once.
    """

    _NOISE = ("failed to receive on socket", "timeout on socket")

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno > logging.WARNING and str(record.msg).startswith(self._NOISE):
            record.levelno, record.levelname = logging.WARNING, logging.getLevelName(logging.WARNING)
        return True


_logger.addFilter(_LinkNoiseFilter())


class LinkError(Exception):
    """
    The connection failed an operation. The library errors for the service are made from it.
    """


class LinkLost(LinkError):
    """
    The connection is broken or could not be made.
    """


class LoginRejected(LinkLost):
    """
    The broker refused the login.
    """


class AckTimeout(LinkError):
    """
    The broker did not confirm in time while the connection still takes packets.
    """


class Connection:
    """
    One connection to the broker, MQTT 5 with a clean session. Every session makes a new one, paho keeps
    unacknowledged messages and would send them after reconnect, so a command the service was told had
    failed would still reach the driver.

    Use it as an async context manager.
    """

    def __init__(self, address: BrokerAddress, identifier: str) -> None:
        self._mqtt_client = aiomqtt.Client(
            address.hostname,
            address.port,
            username=address.username,
            password=address.password,
            transport=address.transport,
            websocket_path=address.websocket_path,
            identifier=identifier,
            # MQTT 5 for the subscription options, numbers of subscriptions and the retain flag as published
            protocol=aiomqtt.ProtocolVersion.V5,
            keepalive=KEEPALIVE_S,
            timeout=ACK_TIMEOUT_S,
            logger=_logger,
        )
        self._mqtt_client.pending_calls_threshold = PENDING_CALLS_WARNING

    async def __aenter__(self) -> "Connection":
        """
        Connect and wait for CONNACK.

        Raises:
            LoginRejected: the broker refused the login.
            LinkLost: the broker did not answer or refused the connection.
        """
        try:
            await self._mqtt_client.__aenter__()
        except aiomqtt.MqttError as error:
            if _is_login_rejected(error):
                raise LoginRejected(str(error)) from error
            raise LinkLost(str(error)) from error
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        try:
            await self._mqtt_client.__aexit__(*exc_info)
        except aiomqtt.MqttError as error:
            raise LinkLost(str(error)) from error

    async def publish(self, topic: str, payload: Payload, qos: int, retain: bool) -> None:
        """
        Publish and wait for PUBACK. The packet reaches paho before the first await.

        Raises:
            AckTimeout: no PUBACK in ACK_TIMEOUT_S while the connection takes packets.
            LinkLost: paho refused the packet, the connection is broken.
        """
        try:
            await self._mqtt_client.publish(topic, payload, qos=qos, retain=retain)
        except aiomqtt.MqttError as error:
            raise _as_link_error(error) from error

    async def subscribe(self, topic_filter: str, qos: int, number: Optional[int] = None) -> None:
        """
        Subscribe and wait for SUBACK. The packet reaches paho before the first await.

        With a number the subscription carries it as its Subscription Identifier and asks for Retain As
        Published, so every message tells the subscriptions it came for and keeps its retain flag.

        Raises:
            LinkError: no SUBACK in time or the connection is broken.
        """
        try:
            if number is None:
                await self._mqtt_client.subscribe(topic_filter, qos=qos)
                return
            properties = Properties(PacketTypes.SUBSCRIBE)
            properties.SubscriptionIdentifier = number
            await self._mqtt_client.subscribe(
                topic_filter,
                options=SubscribeOptions(qos=qos, retainAsPublished=True),
                properties=properties,
            )
        except aiomqtt.MqttError as error:
            raise _as_link_error(error) from error

    async def unsubscribe(self, topic_filter: str) -> None:
        """
        Unsubscribe and wait for UNSUBACK.

        Raises:
            LinkError: no UNSUBACK in time or the connection is broken.
        """
        try:
            await self._mqtt_client.unsubscribe(topic_filter)
        except aiomqtt.MqttError as error:
            raise _as_link_error(error) from error

    async def read_messages(self, handle_message: PacketHandler) -> None:
        """
        Hand every message from the broker to handle_message until the connection ends.

        Raises:
            LinkLost: the connection ended.
        """
        try:
            async for message in self._mqtt_client.messages:
                # The rest of the library takes the payload as bytes
                payload = message.payload
                if not isinstance(payload, (bytes, bytearray)):
                    payload = b"" if payload is None else str(payload).encode()
                handle_message(
                    message.topic.value,
                    bytes(payload),
                    message.retain,
                    message.qos,
                    getattr(message.properties, "SubscriptionIdentifier", None),
                )
        except aiomqtt.MqttError as error:
            raise LinkLost(str(error)) from error
        raise LinkLost("message stream ended")


def _as_link_error(error: aiomqtt.MqttError) -> LinkError:
    if isinstance(error, aiomqtt.MqttCodeError):
        return LinkLost(str(error))
    return AckTimeout(str(error))


def _is_login_rejected(error: aiomqtt.MqttError) -> bool:
    reason_code = getattr(error, "rc", None)
    return getattr(reason_code, "value", reason_code) in _LOGIN_REJECTED_CODES
