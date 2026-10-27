"""
client.raw, for MQTT topics outside the Wiren Board convention.
"""

import asyncio
from typing import Callable

from .._errors import BrokerDisconnectedError, ClientClosedError
from .._listeners import (
    Handler,
    Listener,
    ListenerRegistry,
    MessageHandler,
    check_handler_arguments,
    get_handler_name,
)
from .checks import Payload
from .publisher import Publisher
from .subscriber import SUBSCRIPTION_QOS, Subscriber


class RawTopics:
    """
    client.raw: subscription, one-off read and publication of any topic.

    The client builds it with its own subscriber, publisher and listener registry. is_closed tells whether
    close() of the client was called.
    """

    def __init__(
        self,
        subscriber: Subscriber,
        publisher: Publisher,
        listener_registry: ListenerRegistry,
        *,
        is_closed: Callable[[], bool],
    ) -> None:
        self._subscriber = subscriber
        self._publisher = publisher
        self._listener_registry = listener_registry
        self._is_closed = is_closed
        # The listener of a handler and the filters it listens to
        self._handlers: dict[Handler, tuple[Listener, list[str]]] = {}

    async def subscribe(
        self, topic_filter: str, handler: MessageHandler, qos: int = SUBSCRIPTION_QOS
    ) -> MessageHandler:
        """
        Call handler(message) for every message under the filter, the retained ones first and again
        after every reconnect.

        Returns once the retained messages are queued for the handler, without a link at once. A second
        handler on the same filter gets them too. One handler may listen to several filters with one queue,
        a message that matches several of them comes once. The same handler on the same filter again adds
        nothing.

        Args:
            topic_filter: MQTT filter, + and # allowed.
            handler: function or coroutine function that takes the Message.
            qos: QoS of the subscription. A filter subscribed already keeps the QoS it has.

        Returns:
            The handler, so it can be kept for unsubscribe.

        Raises:
            ValueError: the broker would refuse the filter or the QoS.
            TypeError: the handler does not take one argument.
            ClientClosedError: the client is closed or closes while waiting.
        """
        self._raise_if_closed()
        check_handler_arguments(handler, 1, "raw")
        if handler not in self._handlers:
            listener = self._listener_registry.create_listener(handler, f"raw {get_handler_name(handler)}")
            self._handlers[handler] = (listener, [])
        entry = self._handlers[handler]
        listener, filters = entry
        added = topic_filter not in filters
        if added:
            filters.append(topic_filter)
        try:
            # One bound method per listener, the subscriber calls it once even when filters overlap.
            # A repeated call waits there for the retained messages of the first one.
            await self._subscriber.subscribe([topic_filter], listener.queue_message, qos)
        except BrokerDisconnectedError:
            # Subscribed all the same, the retained messages come after reconnect
            pass
        except BaseException:
            if added:
                filters.remove(topic_filter)
                # unsubscribe may have taken the handler off meanwhile, another call may have added it again
                if not filters and self._handlers.get(handler) is entry:
                    self._stop_listener(handler)
            raise
        return handler

    async def unsubscribe(self, handler: Handler) -> None:
        """
        Stop a handler on all its filters.

        The broker subscription of a filter goes away with its last handler.

        Events the handler has not started are dropped. A call that runs now is cancelled at its next await,
        unless unsubscribe is called from that handler itself. Works after close too.
        """
        if handler not in self._handlers:
            return
        listener, filters = self._stop_listener(handler)
        # A cancelled call still takes the stopped listener off every filter
        await asyncio.shield(self._unsubscribe_filters(listener, filters))

    async def _unsubscribe_filters(self, listener: Listener, filters: list[str]) -> None:
        for topic_filter in filters:
            await self._subscriber.unsubscribe(topic_filter, listener.queue_message)

    async def read(self, topic_filter: str) -> dict[str, bytes]:
        """
        Read what lies in the broker under the filter now. A filter some handler listens to is answered
        without the broker.

        Returns:
            {topic: payload} of the retained messages.

        Raises:
            ValueError: the broker would refuse the filter.
            BrokerDisconnectedError: no broker link.
            ClientClosedError: the client is closed or closes while waiting.
        """
        self._raise_if_closed()
        return await self._subscriber.read_retained([topic_filter])

    async def publish(self, topic: str, payload: Payload, qos: int = 1, retain: bool = False) -> None:
        """
        Publish a message. The arguments go in the order of paho and aiomqtt.

        retain=False is an event, without a link it raises at once. retain=True is state. Without a link
        the call returns at once and the message goes out after reconnect, and the client publishes it again
        after every reconnect, the broker on the controller keeps nothing. An empty retained message erases
        the topic in the broker and ends that.

        Raises:
            ValueError: the broker would refuse the topic or the QoS.
            TypeError: the payload is a bool or not str, bytes, bytearray, int, float or None.
            BrokerDisconnectedError: an event while the broker link is down, or the link is lost
                or the broker does not confirm it in 5 s.
            ClientClosedError: the client is closed or closes while waiting.
        """
        self._raise_if_closed()
        await self._publisher.publish(topic, payload, qos=qos, retain=retain)

    def _stop_listener(self, handler: Handler) -> tuple[Listener, list[str]]:
        listener, filters = self._handlers.pop(handler)
        listener.stop()
        return listener, filters

    def _raise_if_closed(self) -> None:
        if self._is_closed():
            raise ClientClosedError("the client is closed")
