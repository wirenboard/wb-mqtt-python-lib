"""
Subscriptions: hand messages from the broker to their callbacks and tell when retained delivery ends.

The client speaks MQTT 5. Every filter is one broker subscription with its own number, and a live message
keeps the retain flag it was published with. So a subscription knows what the broker keeps under its filter,
and a second callback on the filter gets that from the subscription instead of from the broker.

Ported from wb-mqtt-dali (mqtt_dispatcher.py and retain_hack in wbmqtt.py).
"""

import asyncio
import itertools
import logging
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from . import link, topics
from .errors import BrokerDisconnectedError, ClientClosedError
from .tasks import retrieve_error

logger = logging.getLogger(__name__)

# As wb-rules subscribes. A clean session never gets an unacknowledged message again (MQTT 5, 4.4),
# so QoS 1 would add a PUBACK per message and no delivery guarantee.
SUBSCRIPTION_QOS = 0
# A broker answers in milliseconds, the end of retained delivery waits longer only on a stalled link
RETAINED_END_TIMEOUT_S = 30.0
SLOW_DISPATCH_S = 0.1
# The largest Subscription Identifier of MQTT 5. paho refuses a larger one in every SUBSCRIBE it packs.
MAX_SUBSCRIPTION_NUMBER = 268_435_455
_MARKER_ROOT = "/wbretainhack/"


@dataclass(frozen=True)
class Message:
    """
    Message from the broker.

    retain is True only for the state at subscription, what the broker keeps under the filter when
    the callback subscribes. A live message has it False, even when it was published retained.
    """

    topic: str
    payload: bytes
    retain: bool
    qos: int


MessageCallback = Callable[[Message], None]


@dataclass(eq=False)
class _Subscription:
    number: int
    qos: int
    callbacks: set = field(default_factory=set)
    # What the broker keeps under the filter as far as this session saw it, {topic: payload}
    retained: dict = field(default_factory=dict)
    # Sequence number of the SUBSCRIBE whose retained messages are still arriving,
    # None once a later marker came
    pending_retained: Optional[int] = None


class Subscriber:  # pylint: disable=too-many-instance-attributes
    """
    Hands incoming messages to subscribers, also again after a lost link.

    Callbacks run inside the read loop and must not block, listeners only put events into their queues.
    """

    def __init__(self) -> None:
        self._connection: Optional[link.Connection] = None
        self._subscriptions: dict[str, _Subscription] = {}
        self._by_number: dict[int, _Subscription] = {}
        self._matcher = link.TopicMatcher()
        # How many subscriptions each callback has, a callback with one needs no check for repeats
        self._subscription_counts: Counter[MessageCallback] = Counter()
        self._subscription_numbers = itertools.count(1)
        # Order of SUBSCRIBE and marker packets. The broker handles them in this order, so a marker answers
        # every SUBSCRIBE sent before it.
        self._packet_sequence = itertools.count(1)
        # Subscriptions whose retained messages still arrive. A marker comes with every read of the broker
        # and looks only at these.
        self._pending_retained: set[_Subscription] = set()
        self._marker_prefix = f"{_MARKER_ROOT}{uuid.uuid4().hex}/"
        self._marker_waits: dict[int, asyncio.Future] = {}
        self._connected = False
        self._closing = False
        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def subscription_count(self) -> int:
        return len(self._subscriptions)

    async def connection_restored(self, connection: link.Connection) -> None:
        """
        Subscribe again after (re)connect, the broker drops subscriptions with a clean session.

        The retained messages come to every callback again, publish_resubscription_marker ends them.
        The SUBACK of a subscription comes after the retained messages of the ones before it, a late one
        is no lost link.

        Raises:
            LinkLost: the connection is broken.
        """
        async with self._lock:
            self._connection = connection
            # QoS 1, a QoS 0 marker would pass QoS 1 messages of raw subscriptions the broker still queues
            await connection.subscribe(f"{self._marker_prefix}+", 1)
            # In the order of numbers: a callback takes a live message from its oldest subscription,
            # that one must be at the broker first
            subscriptions = sorted(self._subscriptions.items(), key=lambda item: item[1].number)
            await asyncio.gather(
                *(
                    self._send_subscribe(topic_filter, subscription, late_ack_ok=True)
                    for topic_filter, subscription in subscriptions
                )
            )
            self._connected = True

    async def publish_resubscription_marker(self) -> None:
        """
        Publish the marker that ends the retained messages of connection_restored, without a wait for it.

        Raises:
            LinkLost: the connection is broken.
        """
        if self._pending_retained:
            await self._publish_marker(next(self._packet_sequence))

    def connection_lost(self) -> None:
        """
        Mark the link down.

        What the subscriptions know of the broker is dropped, it may change before the next session.
        Waits for the end of retained delivery fail at once, their marker is lost with the link.
        """
        self._connected = False
        for subscription in self._subscriptions.values():
            subscription.retained.clear()
            subscription.pending_retained = None
        self._pending_retained.clear()
        self._fail_marker_waits(BrokerDisconnectedError("the broker link is lost"))

    def mark_closing(self) -> None:
        """
        The client is closing. A new wait for the broker fails at once, the waits that run go on
        until fail_waits.

        Unsubscriptions from now on stay local. The session ends, and the broker drops its subscriptions.
        """
        self._closing = True

    def fail_waits(self) -> None:
        """
        End every wait for the broker with ClientClosedError.
        """
        self._fail_marker_waits(ClientClosedError("the client is closed"))

    async def subscribe(
        self, topic_filters: Sequence[str], callback: MessageCallback, qos: int = SUBSCRIPTION_QOS
    ) -> None:
        """
        Add a callback for topic filters and return once it got what the broker keeps under them.

        The first callback on a filter subscribes, and the broker sends the retained messages. A callback that
        joins a subscribed filter gets them from the subscription at once, nothing goes to the broker.
        The same callback on the same filter again adds nothing and waits until the retained messages
        of the subscription have arrived.

        Args:
            topic_filters: MQTT filters, + and # allowed.
            callback: called inside the read loop, must not block.
            qos: QoS of a new subscription. A callback that joins gets the QoS the subscription has.

        Raises:
            ValueError: the broker would refuse a filter or the QoS.
            RuntimeError: the client has used up the MQTT subscription numbers, a new client starts anew.
            BrokerDisconnectedError: the link is down or drops before the retained messages arrive.
                The callback stays subscribed and gets them after reconnect.
            ClientClosedError: the client closes while waiting.
        """
        for topic_filter in topic_filters:
            topics.check_filter(topic_filter)
        link.check_qos(qos)
        added: list[str] = []
        try:
            async with self._lock:
                arriving = False
                for topic_filter in topic_filters:
                    subscription = self._subscriptions.get(topic_filter)
                    if subscription is None:
                        subscription = self._add_subscription(topic_filter, qos, callback)
                        added.append(topic_filter)
                        if self._connected:
                            try:
                                await self._send_subscribe(topic_filter, subscription)
                            except link.LinkError as error:
                                # Restored by connection_restored after reconnect
                                logger.debug("Subscription to %s deferred: %s", topic_filter, error)
                    elif callback not in subscription.callbacks:
                        self._add_callback(subscription, callback)
                        added.append(topic_filter)
                    arriving = arriving or subscription.pending_retained is not None
            # close() may have begun while the call waited for the lock or a SUBACK
            if self._closing:
                raise ClientClosedError("the client is closed")
            if not self._connected:
                raise BrokerDisconnectedError("the broker link is down")
            if arriving:
                await self.wait_retained_end()
        except BrokerDisconnectedError:
            raise
        except BaseException:
            for topic_filter in added:
                await self.unsubscribe(topic_filter, callback)
            raise

    async def unsubscribe(
        self, topic_filter: str, callback: MessageCallback, keep: Optional[Callable[[], bool]] = None
    ) -> None:
        """
        Remove a callback. The broker subscription goes away with the last callback.

        Args:
            keep: asked once the change of subscriptions may go on, True leaves the callback subscribed.
        """
        async with self._lock:
            if keep is not None and keep():
                return
            subscription = self._subscriptions.get(topic_filter)
            if subscription is None or callback not in subscription.callbacks:
                return
            subscription.callbacks.discard(callback)
            self._subscription_counts[callback] -= 1
            if not self._subscription_counts[callback]:
                del self._subscription_counts[callback]
            if subscription.callbacks:
                return
            del self._subscriptions[topic_filter]
            del self._matcher[topic_filter]
            del self._by_number[subscription.number]
            self._pending_retained.discard(subscription)
            # The cleanup of a call that close ended would wait for an answer that never comes
            if self._connected and not self._closing:
                try:
                    await self._connection.unsubscribe(topic_filter)
                except link.LinkError as error:
                    logger.debug("Unsubscribe from %s skipped: %s", topic_filter, error)

    def dispatch(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, topic: str, payload: bytes, retain: bool, qos: int, numbers: Optional[list[int]]
    ) -> None:
        """
        Hand a message from the broker to the callbacks of the subscriptions it came for, once per callback.

        retain is the flag the message was published with, numbers are the subscriptions it came for.
        """
        if topic.startswith(self._marker_prefix):
            self._marker_arrived(topic)
            return
        started = time.monotonic()
        if numbers:
            subscriptions = [self._by_number[number] for number in numbers if number in self._by_number]
        else:
            subscriptions = list(self._matcher.iter_match(topic))
        matching: list[_Subscription] = []
        for subscription in subscriptions:
            self._dispatch_to(subscription, topic, payload, retain, qos, matching)
        elapsed = time.monotonic() - started
        if elapsed > SLOW_DISPATCH_S:
            logger.warning("Dispatching %s took %.2f s", topic, elapsed)

    async def wait_retained_end(self) -> None:
        """
        Return once the broker has delivered the retained messages of the subscriptions made so far.

        The broker answers in order, so a marker published now arrives after them.

        Raises:
            BrokerDisconnectedError: the link is down or drops while waiting, or the broker does not answer
                in RETAINED_END_TIMEOUT_S.
            ClientClosedError: the client closes.
        """
        if self._closing:
            raise ClientClosedError("the client is closed")
        if not self._connected:
            raise BrokerDisconnectedError("the broker link is down")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + RETAINED_END_TIMEOUT_S
        sequence = next(self._packet_sequence)
        arrived = loop.create_future()
        self._marker_waits[sequence] = arrived
        try:
            try:
                await self._publish_marker(sequence)
            except link.LinkLost as error:
                raise BrokerDisconnectedError(f"the end of retained delivery is unknown: {error}") from error
            try:
                await asyncio.wait_for(arrived, max(deadline - loop.time(), 0))
            except TimeoutError:
                # For the service a silent broker is a lost link
                raise BrokerDisconnectedError(
                    f"the broker did not answer in {RETAINED_END_TIMEOUT_S:g} s"
                ) from None
        finally:
            del self._marker_waits[sequence]
            if arrived.done():
                retrieve_error(arrived)

    async def read_retained(self, topic_filters: list[str]) -> dict[str, bytes]:
        """
        Read what lies in the broker under the filters now, without keeping the subscription.

        A filter that is subscribed already is answered by its subscription, without the broker.

        Raises:
            BrokerDisconnectedError: the link is down or drops while reading.
            ClientClosedError: the client closes while reading.
        """

        def hold(_message: Message) -> None:
            pass

        try:
            await self.subscribe(topic_filters, hold)
            # Once the retained messages arrived, a subscription knows what the broker keeps, with the live
            # erasures and changes that came meanwhile
            return {
                topic: payload
                for topic_filter in topic_filters
                for topic, payload in self._subscriptions[topic_filter].retained.items()
            }
        finally:
            for topic_filter in topic_filters:
                await self.unsubscribe(topic_filter, hold)

    def _add_subscription(self, topic_filter: str, qos: int, callback: MessageCallback) -> _Subscription:
        number = next(self._subscription_numbers)
        if number > MAX_SUBSCRIPTION_NUMBER:
            # Kept in the client, it would end every session at the resubscription after reconnect
            raise RuntimeError("the client has used up the MQTT subscription numbers")
        subscription = _Subscription(number, qos, {callback})
        self._subscriptions[topic_filter] = subscription
        self._matcher[topic_filter] = subscription
        self._by_number[subscription.number] = subscription
        self._subscription_counts[callback] += 1
        return subscription

    def _add_callback(self, subscription: _Subscription, callback: MessageCallback) -> None:
        subscription.callbacks.add(callback)
        self._subscription_counts[callback] += 1
        for topic, payload in list(subscription.retained.items()):
            matching: list[_Subscription] = []
            if not self._takes_from(callback, subscription, topic, matching):
                continue
            # A topic that another subscription of the callback delivered already is not handed over again.
            # That one may still wait for its marker, what it delivered so far is in its retained.
            if self._subscription_counts[callback] > 1 and any(
                item is not subscription and topic in item.retained
                for item in self._subscriptions_of(callback, topic, matching)
            ):
                continue
            self._call_safely(callback, Message(topic, payload, True, subscription.qos))

    async def _send_subscribe(
        self, topic_filter: str, subscription: _Subscription, late_ack_ok: bool = False
    ) -> None:
        # Nothing awaits between the sequence number and the packet, so the numbers follow the packets
        subscription.pending_retained = next(self._packet_sequence)
        self._pending_retained.add(subscription)
        try:
            await self._connection.subscribe(topic_filter, subscription.qos, subscription.number)
        except link.AckTimeout as error:
            if not late_ack_ok:
                raise
            logger.debug("SUBACK for %s is late: %s", topic_filter, error)

    async def _publish_marker(self, sequence: int) -> None:
        """
        Publish a marker. Its PUBACK comes after the retained messages before it, a late one is no lost link,
        the marker itself tells when they end.

        Raises:
            LinkLost: the connection is broken.
        """
        try:
            await self._connection.publish(self._marker_topic(sequence), "2", 1, False)
        except link.AckTimeout as error:
            logger.debug("PUBACK of marker %d is late: %s", sequence, error)

    def _dispatch_to(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        subscription: _Subscription,
        topic: str,
        payload: bytes,
        retain: bool,
        qos: int,
        matching: list[_Subscription],
    ) -> None:
        """
        Hand a message to the callbacks of the subscription.

        retain is the flag the message was published with.
        """
        # The broker never sends an empty payload on subscription, so an empty one is a live erasure
        if retain and payload:
            subscription.retained[topic] = payload
        elif retain:
            subscription.retained.pop(topic, None)
        initial = subscription.pending_retained is not None and retain and bool(payload)
        message = Message(topic, payload, initial, qos)
        for callback in list(subscription.callbacks):
            if self._takes_from(callback, subscription, topic, matching):
                self._call_safely(callback, message)

    def _takes_from(
        self,
        callback: MessageCallback,
        subscription: _Subscription,
        topic: str,
        matching: list[_Subscription],
    ) -> bool:
        """
        Whether the callback takes the topic from this subscription and not from another overlapping one.

        The broker sends a copy per matching subscription. A callback takes every topic from one subscription
        of its own, the oldest of those that got their retained messages, else the oldest at all.
        """
        if self._subscription_counts[callback] < 2:
            return True
        candidates = self._subscriptions_of(callback, topic, matching)
        source = min(candidates, key=lambda item: (item.pending_retained is not None, item.number))
        return source is subscription

    def _subscriptions_of(
        self, callback: MessageCallback, topic: str, matching: list[_Subscription]
    ) -> list[_Subscription]:
        # matching keeps the subscriptions of the topic for one message, looked up once
        if not matching:
            matching.extend(self._matcher.iter_match(topic))
        return [item for item in matching if callback in item.callbacks]

    def _marker_arrived(self, topic: str) -> None:
        try:
            sequence = int(topic[len(self._marker_prefix) :])
        except ValueError:
            return
        for subscription in [item for item in self._pending_retained if item.pending_retained < sequence]:
            subscription.pending_retained = None
            self._pending_retained.discard(subscription)
        for marker_sequence, arrived in self._marker_waits.items():
            if marker_sequence <= sequence and not arrived.done():
                arrived.set_result(None)

    def _marker_topic(self, sequence: int) -> str:
        return f"{self._marker_prefix}{sequence}"

    def _fail_marker_waits(self, error: Exception) -> None:
        for arrived in self._marker_waits.values():
            if not arrived.done():
                arrived.set_exception(error)

    @staticmethod
    def _call_safely(callback: MessageCallback, message: Message) -> None:
        try:
            callback(message)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Callback for %s failed", message.topic)
