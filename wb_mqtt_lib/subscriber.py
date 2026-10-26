"""
Subscriptions: hand messages from the broker to their callbacks and tell when retained delivery ends.

The client uses MQTT 5. Every filter is one broker subscription with its own number, and a live message
keeps the retain flag it was published with. So a subscription knows what the broker keeps under its filter,
and a second callback on the filter gets that from the subscription instead of from the broker.
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
# Dispatching one message longer than this goes to the log, callbacks must not block
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
    A callback subscribed with retain_as_published gets the flag the message was published with.
    """

    topic: str
    payload: bytes
    retain: bool
    qos: int


# A callback of a subscription, it takes a Message and returns nothing
MessageCallback = Callable[[Message], None]


@dataclass(eq=False)
class _Subscription:
    number: int
    qos: int
    callbacks: set[MessageCallback] = field(default_factory=set)
    # What the broker keeps under the filter as far as this session saw it, {topic: payload}
    retained: dict[str, bytes] = field(default_factory=dict)
    # Packet number of the SUBSCRIBE whose retained messages are still arriving,
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
        self._subscriptions_by_number: dict[int, _Subscription] = {}
        self._topic_matcher = link.TopicMatcher()
        # How many subscriptions each callback has, a callback with one needs no check for repeats
        self._subscription_counts: Counter[MessageCallback] = Counter()
        # Whether a callback gets the retain flag a message was published with
        self._retain_as_published: dict[MessageCallback, bool] = {}
        self._subscription_numbers = itertools.count(1)
        # Order of SUBSCRIBE and marker packets. The broker handles them in this order, so a marker answers
        # every SUBSCRIBE sent before it.
        self._packet_numbers = itertools.count(1)
        # Subscriptions whose retained messages still arrive. A marker comes with every read of the broker
        # and looks only at these.
        self._pending_retained: set[_Subscription] = set()
        self._marker_prefix = f"{_MARKER_ROOT}{uuid.uuid4().hex}/"
        # Waits for a marker by its packet number
        self._marker_waits: dict[int, asyncio.Future] = {}
        self._connected = False
        self._closing = False
        # One change of subscriptions at a time, the resubscription after reconnect too
        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def subscription_count(self) -> int:
        return len(self._subscriptions)

    # ------------------------------------------------------------
    # Connection and closing
    # ------------------------------------------------------------

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
            # In the order of numbers. A callback takes a live message from its oldest subscription,
            # that one must be at the broker first.
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
            await self._publish_marker(next(self._packet_numbers))

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

    # ------------------------------------------------------------
    # Subscriptions and reads
    # ------------------------------------------------------------

    async def subscribe(
        self,
        topic_filters: Sequence[str],
        callback: MessageCallback,
        qos: int = SUBSCRIPTION_QOS,
        retain_as_published: bool = False,
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
            retain_as_published: the callback gets the retain flag a message was published with. Otherwise
                retain is True only for the state at subscription, as a raw handler sees it.

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
        self._retain_as_published[callback] = retain_as_published
        added_filters: list[str] = []
        try:
            # A new filter subscribes, a known one takes the callback and hands it the retained messages
            async with self._lock:
                retained_arriving = False
                for topic_filter in topic_filters:
                    subscription = self._subscriptions.get(topic_filter)
                    if subscription is None:
                        subscription = self._add_subscription(topic_filter, qos, callback)
                        added_filters.append(topic_filter)
                        if self._connected:
                            try:
                                await self._send_subscribe(topic_filter, subscription)
                            except link.LinkError as error:
                                # Restored by connection_restored after reconnect
                                logger.debug("Subscription to %s deferred: %s", topic_filter, error)
                    elif callback not in subscription.callbacks:
                        self._add_callback(subscription, callback)
                        added_filters.append(topic_filter)
                    retained_arriving = retained_arriving or subscription.pending_retained is not None

            # close() may have begun while the call waited for the lock or a SUBACK
            if self._closing:
                raise ClientClosedError("the client is closed")
            if not self._connected:
                raise BrokerDisconnectedError("the broker link is down")
            if retained_arriving:
                await self.wait_retained_end()

        # Without a link the callback stays subscribed, any other failure takes back what the call added
        except BrokerDisconnectedError:
            raise
        except BaseException:
            for topic_filter in added_filters:
                await self.unsubscribe(topic_filter, callback)
            raise

    async def unsubscribe(
        self,
        topic_filter: str,
        callback: MessageCallback,
        still_needed: Optional[Callable[[], bool]] = None,
    ) -> None:
        """
        Remove a callback. The broker subscription goes away with the last callback.

        Args:
            still_needed: a function without arguments, called under the lock right before the removal.
                True leaves the callback subscribed. It is for a caller whose reason to unsubscribe may be
                gone while it waited for the lock.
        """
        async with self._lock:
            if still_needed is not None and still_needed():
                return

            # The callback leaves the subscription
            subscription = self._subscriptions.get(topic_filter)
            if subscription is None or callback not in subscription.callbacks:
                return
            subscription.callbacks.discard(callback)
            self._subscription_counts[callback] -= 1
            if not self._subscription_counts[callback]:
                del self._subscription_counts[callback]
                self._retain_as_published.pop(callback, None)
            if subscription.callbacks:
                return

            # The subscription goes with its last callback
            del self._subscriptions[topic_filter]
            del self._topic_matcher[topic_filter]
            del self._subscriptions_by_number[subscription.number]
            self._pending_retained.discard(subscription)
            # The cleanup of a call that close ended would wait for an answer that never comes
            if self._connected and not self._closing:
                try:
                    await self._connection.unsubscribe(topic_filter)
                except link.LinkError as error:
                    logger.debug("Unsubscribe from %s skipped: %s", topic_filter, error)

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

        # A marker after the retained messages, its arrival ends the wait
        loop = asyncio.get_running_loop()
        deadline = loop.time() + RETAINED_END_TIMEOUT_S
        marker_number = next(self._packet_numbers)
        marker_received = loop.create_future()
        self._marker_waits[marker_number] = marker_received
        try:
            try:
                await self._publish_marker(marker_number)
            except link.LinkLost as error:
                raise BrokerDisconnectedError(f"the end of retained delivery is unknown: {error}") from error
            try:
                await asyncio.wait_for(marker_received, max(deadline - loop.time(), 0))
            except TimeoutError:
                # For the service a silent broker is a lost link
                raise BrokerDisconnectedError(
                    f"the broker did not answer in {RETAINED_END_TIMEOUT_S:g} s"
                ) from None
        finally:
            del self._marker_waits[marker_number]
            if marker_received.done():
                retrieve_error(marker_received)

    async def read_retained(self, topic_filters: list[str]) -> dict[str, bytes]:
        """
        Read what lies in the broker under the filters now, without keeping the subscription.

        A filter that is subscribed already is answered by its subscription, without the broker.

        Raises:
            BrokerDisconnectedError: the link is down or drops while reading.
            ClientClosedError: the client closes while reading.
        """

        # Keeps the subscriptions for the time of the read
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

    # ------------------------------------------------------------
    # Messages from the broker
    # ------------------------------------------------------------

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

        # The subscriptions the message came for, by their numbers or by the filters
        started = time.monotonic()
        if numbers:
            subscriptions = [
                self._subscriptions_by_number[number]
                for number in numbers
                if number in self._subscriptions_by_number
            ]
        else:
            subscriptions = list(self._topic_matcher.iter_match(topic))
        matching: list[_Subscription] = []
        for subscription in subscriptions:
            self._dispatch_to(subscription, topic, payload, retain, qos, matching)

        elapsed = time.monotonic() - started
        if elapsed > SLOW_DISPATCH_S:
            logger.warning("Dispatching %s took %.2f s", topic, elapsed)

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
        # What the broker keeps. On subscription it never sends an empty payload, so that is a live erasure.
        if retain and payload:
            subscription.retained[topic] = payload
        elif retain:
            subscription.retained.pop(topic, None)

        # Callbacks see retain only for the state at subscription, unless they asked for it as published
        retained_at_subscription = subscription.pending_retained is not None and retain and bool(payload)
        message = Message(topic, payload, retained_at_subscription, qos)
        message_as_published = Message(topic, payload, retain, qos)
        for callback in list(subscription.callbacks):
            if not self._is_source_subscription(callback, subscription, topic, matching):
                continue
            if self._retain_as_published.get(callback):
                self._call_safely(callback, message_as_published)
            else:
                self._call_safely(callback, message)

    def _is_source_subscription(
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
        candidates = self._get_callback_subscriptions(callback, topic, matching)
        source = min(candidates, key=lambda item: (item.pending_retained is not None, item.number))
        return source is subscription

    def _get_callback_subscriptions(
        self, callback: MessageCallback, topic: str, matching: list[_Subscription]
    ) -> list[_Subscription]:
        # matching keeps the subscriptions of the topic for one message, looked up once
        if not matching:
            matching.extend(self._topic_matcher.iter_match(topic))
        return [item for item in matching if callback in item.callbacks]

    def _marker_arrived(self, topic: str) -> None:
        try:
            marker_number = int(topic[len(self._marker_prefix) :])
        except ValueError:
            return

        # The retained messages of every SUBSCRIBE sent before the marker have arrived
        for subscription in [
            item for item in self._pending_retained if item.pending_retained < marker_number
        ]:
            subscription.pending_retained = None
            self._pending_retained.discard(subscription)

        # So have those of the waits that published this marker or an earlier one
        for waited_number, marker_received in self._marker_waits.items():
            if waited_number <= marker_number and not marker_received.done():
                marker_received.set_result(None)

    @staticmethod
    def _call_safely(callback: MessageCallback, message: Message) -> None:
        try:
            callback(message)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Callback for %s failed", message.topic)

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------

    def _add_subscription(self, topic_filter: str, qos: int, callback: MessageCallback) -> _Subscription:
        number = next(self._subscription_numbers)
        if number > MAX_SUBSCRIPTION_NUMBER:
            # Kept in the client, it would end every session at the resubscription after reconnect
            raise RuntimeError("the client has used up the MQTT subscription numbers")
        subscription = _Subscription(number, qos, {callback})
        self._subscriptions[topic_filter] = subscription
        self._topic_matcher[topic_filter] = subscription
        self._subscriptions_by_number[subscription.number] = subscription
        self._subscription_counts[callback] += 1
        return subscription

    def _add_callback(self, subscription: _Subscription, callback: MessageCallback) -> None:
        subscription.callbacks.add(callback)
        self._subscription_counts[callback] += 1

        # The callback gets what the subscription knows of the broker
        for topic, payload in list(subscription.retained.items()):
            matching: list[_Subscription] = []
            if not self._is_source_subscription(callback, subscription, topic, matching):
                continue
            # A topic that another subscription of the callback delivered already is not handed over again.
            # That one may still wait for its marker, what it delivered so far is in its retained.
            if self._subscription_counts[callback] > 1 and any(
                item is not subscription and topic in item.retained
                for item in self._get_callback_subscriptions(callback, topic, matching)
            ):
                continue
            self._call_safely(callback, Message(topic, payload, True, subscription.qos))

    async def _send_subscribe(
        self, topic_filter: str, subscription: _Subscription, late_ack_ok: bool = False
    ) -> None:
        # Nothing awaits between the number and the packet, so the numbers follow the packets
        subscription.pending_retained = next(self._packet_numbers)
        self._pending_retained.add(subscription)
        try:
            await self._connection.subscribe(topic_filter, subscription.qos, subscription.number)
        except link.AckTimeout as error:
            if not late_ack_ok:
                raise
            logger.debug("SUBACK for %s is late: %s", topic_filter, error)

    async def _publish_marker(self, marker_number: int) -> None:
        """
        Publish a marker. Its PUBACK comes after the retained messages before it, a late one is no lost link,
        the marker itself tells when they end.

        Raises:
            LinkLost: the connection is broken.
        """
        try:
            await self._connection.publish(self._get_marker_topic(marker_number), "2", 1, False)
        except link.AckTimeout as error:
            logger.debug("PUBACK of marker %d is late: %s", marker_number, error)

    def _get_marker_topic(self, marker_number: int) -> str:
        return f"{self._marker_prefix}{marker_number}"

    def _fail_marker_waits(self, error: Exception) -> None:
        for marker_received in self._marker_waits.values():
            if not marker_received.done():
                marker_received.set_exception(error)
