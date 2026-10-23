"""
Listeners. Each one has its own queue and task.

Its events go in order and other listeners never wait for it.
"""

import asyncio
import inspect
import logging
import weakref
from typing import Any, Awaitable, Callable, Iterable, Optional, TypeVar, Union

from .tasks import cancelled_from_outside, log_failure

logger = logging.getLogger(__name__)

# Events waiting in one queue before the log says the handler cannot keep up
BACKLOG_WARNING = 100

HandlerResult = Union[None, Awaitable[None]]
# handler(control), for on_added, on_removed, on_meta, on_control_added, on_control_removed, on_control_meta
ControlHandler = Callable[[Any], HandlerResult]
# handler(device), for on_added, on_removed and on_meta of a device
DeviceHandler = Callable[[Any], HandlerResult]
# handler(control, value), for on_command
CommandHandler = Callable[[Any, Any], HandlerResult]
# handler(control, value, previous), for on_value, on_error, on_control_value, on_control_error,
# and handler(device, error, previous) for on_error of a device
ChangeHandler = Callable[[Any, Any, Any], HandlerResult]
# handler(message), for client.raw
MessageHandler = Callable[[Any], HandlerResult]
# handler(connected), for on_connection_change of the client
ConnectionHandler = Callable[[bool], HandlerResult]

Handler = Callable[..., Any]
HandlerT = TypeVar("HandlerT", bound=Handler)
# The registration methods return the handler they got, so they also work as decorators
ControlHandlerT = TypeVar("ControlHandlerT", bound=ControlHandler)
DeviceHandlerT = TypeVar("DeviceHandlerT", bound=DeviceHandler)
CommandHandlerT = TypeVar("CommandHandlerT", bound=CommandHandler)
ChangeHandlerT = TypeVar("ChangeHandlerT", bound=ChangeHandler)
MessageHandlerT = TypeVar("MessageHandlerT", bound=MessageHandler)
ConnectionHandlerT = TypeVar("ConnectionHandlerT", bound=ConnectionHandler)


def check_handler_arguments(handler: Handler, count: int, what: str) -> None:
    """
    Make sure the handler takes the arguments its events bring.

    Otherwise a mistake shows only at the first event, which for on_removed may come weeks later.
    Callables without a signature are let through.

    Raises:
        TypeError: the handler does not take that many positional arguments.
    """
    try:
        signature = inspect.signature(handler)
    except (TypeError, ValueError):
        return
    try:
        signature.bind(*([None] * count))
    except TypeError as error:
        raise TypeError(
            f"{what} handler {handler_name(handler)} must take {count} arguments: {error}"
        ) from None


def handler_name(handler: Handler) -> str:
    return getattr(handler, "__qualname__", repr(handler))


class Listener:
    """
    One handler with its queue. The handler may be a plain function or a coroutine function.

    Every event reaches the handler with the arguments it had when it happened, in order, as in wb-rules.
    The task starts with the first event, so a listener may be created before the event loop runs.
    """

    def __init__(self, handler: Handler, description: str, registry: "ListenerRegistry") -> None:
        self.handler = handler
        self._description = description
        self._registry = registry
        self._queue: asyncio.Queue[tuple] = asyncio.Queue()
        self._backlog_reported = False
        self._stopped = False
        self._task: Optional[asyncio.Task] = None

    def push(self, args: tuple) -> None:
        self._queue.put_nowait(args)
        if self._task is None and not self._stopped:
            self._task = asyncio.create_task(self._run(), name=f"listener {self._description}")
            self._registry.track_task(self._task)
        if self._queue.qsize() >= BACKLOG_WARNING and not self._backlog_reported:
            self._backlog_reported = True
            logger.warning("Listener %s has %d events waiting", self._description, self._queue.qsize())

    def push_message(self, message: Any) -> None:
        """
        Queue a call with one message, the form subscriber callbacks have.
        """
        self.push((message,))

    @property
    def task(self) -> Optional[asyncio.Task]:
        """
        The task that calls the handler, None until the first event.
        """
        return self._task

    def stop(self, spare: Optional[asyncio.Task] = None) -> None:
        """
        Stop the listener.

        Events still in the queue are dropped and a call that runs now is cancelled at its next await.
        Called from the handler itself, or with spare being its task, the call finishes and the listener
        stops after it.
        """
        self._registry.discard(self)
        # A handler that catches the cancellation still ends the listener with its current call
        self._stopped = True
        # Cancelling the task that calls this would break the handler in the middle, for example of close()
        if self._task is not None and self._task not in (asyncio.current_task(), spare):
            self._task.cancel()

    async def _run(self) -> None:
        while not self._stopped:
            args = await self._queue.get()
            try:
                result = self.handler(*args)
                if inspect.isawaitable(result):
                    await result
            except Exception as error:  # pylint: disable=broad-exception-caught
                log_failure(logger, self._failure_label(args), error)
            except asyncio.CancelledError as error:
                # A CancelledError of a task the handler awaited is a failure of the handler, not a stop
                if cancelled_from_outside():
                    raise
                log_failure(logger, self._failure_label(args), error)
            if self._backlog_reported and self._queue.empty():
                self._backlog_reported = False

    def _failure_label(self, args: tuple) -> str:
        # A raw handler gets a message, its topic tells which one broke the handler
        topic = getattr(args[0], "topic", None) if args else None
        on_topic = f" on {topic}" if isinstance(topic, str) else ""
        return f"Listener {self._description}{on_topic}"


class ListenerRegistry:
    """
    All listeners of a client, so the client can stop them.
    """

    def __init__(self) -> None:
        self._listeners: set[Listener] = set()
        # Tasks of the listeners, also of those stopped already, so close() knows a handler that calls it
        self._tasks: weakref.WeakSet[asyncio.Task] = weakref.WeakSet()

    def __len__(self) -> int:
        return len(self._listeners)

    def track_task(self, task: asyncio.Task) -> None:
        self._tasks.add(task)

    def create_listener(self, handler: Handler, description: str) -> Listener:
        listener = Listener(handler, description, self)
        self._listeners.add(listener)
        return listener

    def discard(self, listener: Listener) -> None:
        self._listeners.discard(listener)

    def is_listener_task(self, task: Optional[asyncio.Task]) -> bool:
        """
        Whether the task calls a handler of one of these listeners.
        """
        return task is not None and task in self._tasks

    async def stop_all(self, spare: Optional[asyncio.Task] = None) -> None:
        """
        Stop every listener and wait until their tasks end, except the task that calls this and spare.

        The spared listener stops after its current call.
        """
        listeners = list(self._listeners)
        current = asyncio.current_task()
        for listener in listeners:
            listener.stop(spare)
        tasks = [listener.task for listener in listeners if listener.task not in (None, current, spare)]
        await asyncio.gather(*tasks, return_exceptions=True)


class ListenerSet:
    """
    Listeners of one device, control or client by event kind. Registering the same handler twice adds nothing.
    """

    def __init__(self, registry: ListenerRegistry, owner: str) -> None:
        self._registry = registry
        self._owner = owner
        self._by_kind: dict[str, dict[Handler, Listener]] = {}

    def __bool__(self) -> bool:
        return any(self._by_kind.values())

    def add(self, kind: str, handler: HandlerT, initial: Iterable[tuple] = ()) -> HandlerT:
        """
        Register a handler and queue the calls that describe the current state.
        """
        listeners = self._by_kind.setdefault(kind, {})
        if handler in listeners:
            return handler
        listener = self._registry.create_listener(handler, f"{kind} of {self._owner}")
        for args in initial:
            listener.push(args)
        listeners[handler] = listener
        return handler

    def remove(self, handler: Handler) -> None:
        """
        Unregister a handler from every event kind.
        """
        for listeners in self._by_kind.values():
            listener: Optional[Listener] = listeners.pop(handler, None)
            if listener is not None:
                listener.stop()

    def push(self, kind: str, args: tuple) -> None:
        for listener in self._by_kind.get(kind, {}).values():
            listener.push(args)

    def has(self, kind: str) -> bool:
        return bool(self._by_kind.get(kind))
