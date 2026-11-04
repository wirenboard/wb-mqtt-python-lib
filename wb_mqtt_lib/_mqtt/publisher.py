"""
Publication and the own retained state that survives a lost broker link.

Own retained state lives in the mirror, what should lie in the broker. A change goes into the mirror at once
and into the queue of the session in the order of the calls, the queue hands it to the broker. So a cancelled
call never leaves a change half sent, and a change made later never goes out before an earlier one.
After reconnect a new session queues the whole mirror again, the broker on the controller keeps nothing.
"""

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import NamedTuple, Optional, Sequence

from .._errors import BrokerDisconnectedError, ClientClosedError
from .._logs import get_logger
from .._tasks import retrieve_error
from . import broker_connection, checks

logger = get_logger(__name__)

# QoS of the publications of the library, the broker confirms each of them
PUBLICATION_QOS = 1


class _Message(NamedTuple):
    """
    One message for Publisher._put. A later stage of an erasure goes out after the broker confirmed
    the earlier one.
    """

    topic: str
    payload: checks.Payload
    stage: int = 0


@dataclass(frozen=True)
class _MirrorRecord:
    payload: checks.Payload
    qos: int
    # Stage of an erasure, a later stage goes out after the broker confirmed the earlier one
    stage: int = 0


class _Batch:
    """
    Publications of one call. The future ends when each of them is confirmed or left to the replay,
    and fails with the error of a publication that failed, ClientClosedError for one left unconfirmed
    by close().
    """

    __slots__ = ("future", "unfinished")

    def __init__(self, count: int) -> None:
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()
        # A caller that was cancelled no longer waits, nobody reads the error then
        self.future.add_done_callback(retrieve_error)
        self.unfinished = count
        if not count:
            self.future.set_result(None)

    def publication_finished(self) -> None:
        self.unfinished -= 1
        if self.unfinished <= 0 and not self.future.done():
            self.future.set_result(None)

    def fail(self, error: Exception) -> None:
        if not self.future.done():
            self.future.set_exception(error)


@dataclass(eq=False)
class _Publication:  # pylint: disable=too-many-instance-attributes
    """
    One publication in the queue of a session.
    """

    topic: str
    payload: checks.Payload
    qos: int
    retain: bool
    # The mirror record a retained publication carries. One whose record is no longer the current one
    # of its topic is out of date, a newer change has a publication of its own further back in the queue.
    mirror_record: Optional[_MirrorRecord] = None
    batch: Optional[_Batch] = None
    # Queued by the replay after reconnect, skipped when a newer change overtook it
    replay: bool = False
    finished: bool = False
    # An unexpected error of paho or aiomqtt while sending
    error: Optional[Exception] = None

    def finish(self) -> None:
        """
        The caller stops waiting for it, the broker confirmed it or it is left to the replay.
        """
        if not self.finished:
            self.finished = True
            if self.batch is not None:
                self.batch.publication_finished()

    def fail(self, error: Exception) -> None:
        """
        The caller gets the error, the publication is not confirmed and will not be.
        """
        if not self.finished:
            self.finished = True
            if self.batch is not None:
                self.batch.fail(error)


# In the queue, what follows waits until the broker confirmed everything before it
_BARRIER = None


class _Session:  # pylint: disable=too-few-public-methods
    """
    Publication over one broker connection. A new connection starts a new session with an empty queue.
    """

    def __init__(self, connection: broker_connection.Connection) -> None:
        self.connection = connection
        self.loop = asyncio.get_running_loop()
        self.queue: deque[Optional[_Publication]] = deque()
        # Publications handed to the connection and waiting for PUBACK, at most broker_connection.SEND_WINDOW
        self.inflight = 0
        self.send_tasks: set[asyncio.Task] = set()


class Publisher:
    """
    Publishes messages and the own retained state, also again after a lost link.
    """

    def __init__(self) -> None:
        self._session: Optional[_Session] = None
        self._mirror: dict[str, _MirrorRecord] = {}
        # Send tasks of the session that ended, cancelled and not finished yet
        self._ended_session_tasks: list[asyncio.Task] = []
        # _send_queued runs, a call from inside it returns at once
        self._sending_queued = False
        self._closing = False

    @property
    def mirror_size(self) -> int:
        return len(self._mirror)

    # ------------------------------------------------------------
    # Connection and closing
    # ------------------------------------------------------------

    def mark_closing(self) -> None:
        """
        The client is closing, there is no replay after it. Publications the broker has not confirmed
        by the end of the session fail with ClientClosedError, retained ones too.
        """
        self._closing = True

    def connection_restored(self, connection: broker_connection.Connection) -> None:
        """
        Start a session over the new connection and queue the own retained state.
        """
        self._session = _Session(connection)
        self._queue_replay(self._session)

    def connection_lost(self) -> None:
        """
        End the session. Publications it has not got confirmed are left to the replay, events among them
        fail, and after mark_closing all of them fail with ClientClosedError. Its send tasks are cancelled,
        wait_ended_session waits for them.
        """
        session, self._session = self._session, None
        if session is None:
            return
        for publication in session.queue:
            if publication is not _BARRIER:
                self._end_unconfirmed(publication)
        session.queue.clear()
        for task in session.send_tasks:
            task.cancel()
        self._ended_session_tasks = list(session.send_tasks)

    async def wait_ended_session(self) -> None:
        """
        Wait until the send tasks of the session that ended are gone.
        """
        ended_tasks, self._ended_session_tasks = self._ended_session_tasks, []
        await asyncio.gather(*ended_tasks, return_exceptions=True)

    # ------------------------------------------------------------
    # Publication
    # ------------------------------------------------------------

    async def publish(
        self, topic: str, payload: checks.Payload = None, qos: int = PUBLICATION_QOS, retain: bool = False
    ) -> None:
        """
        Publish a message and return once the broker confirmed it.

        A retained message is state, without a link it waits in the mirror and goes out after reconnect.
        When the broker does not confirm it in broker_connection.ACK_TIMEOUT_S while the link lives, the call
        returns as well and the message goes again in its place. A non-retained message is an event, without
        a link it is an error. A cancelled call does not take the message back.

        Raises:
            ValueError: the topic or the QoS is invalid.
            TypeError: the payload is a bool or not str, bytes, bytearray, int, float or None.
            BrokerDisconnectedError: an event while the link is down, or the link is lost or the broker
                does not confirm it in broker_connection.ACK_TIMEOUT_S.
            ClientClosedError: the client closes before the broker confirmed the message.
            Exception: an unexpected error of paho or aiomqtt, as it is. A retained message is taken back.
        """
        if not isinstance(topic, checks.CheckedTopic):
            checks.check_topic(topic)
        checks.check_qos(qos)
        checks.check_payload(payload)
        if isinstance(payload, bytearray):
            # A buffer the service changes later must not change what goes out after reconnect
            payload = bytes(payload)
        await asyncio.shield(self._put([_Message(topic, payload)], qos, retain=retain, wait_for_puback=True))

    def put_event(self, topic: str, payload: checks.Payload) -> asyncio.Future:
        """
        Put an own event, a message without retain, into the queue, QoS 1. The topic and the payload are
        checked by the caller.

        Returns:
            A future that ends when the broker confirmed the message. It fails with BrokerDisconnectedError
            when the link is lost or the broker does not confirm it in broker_connection.ACK_TIMEOUT_S,
            and with ClientClosedError when the client closes before that.

        Raises:
            BrokerDisconnectedError: the link is down.
            Exception: an unexpected error of paho or aiomqtt on the way to paho, as it is.
        """
        return self._put([_Message(topic, payload)], PUBLICATION_QOS, retain=False, wait_for_puback=True)

    def put_retained(
        self, messages: Sequence[tuple[str, checks.Payload]], *, wait_for_puback: bool = True
    ) -> asyncio.Future:
        """
        Put own retained state into the mirror and the queue in the order given, QoS 1. The queue hands it
        to paho while the window has room, without a wait. A caller changes its own state after the call,
        so an error of paho leaves that state as it was.

        Returns:
            A future that ends when the broker confirmed every message or they are left to the replay, and
            fails with ClientClosedError when the client closes before that. Without wait_for_puback it is
            done already when paho took the messages at once. With a full window or a queue it still waits
            for the broker, so a service cannot run ahead of it.

        Raises:
            Exception: an unexpected error of paho or aiomqtt on the way to paho, as it is. The mirror gets
                back what it had before the call, and what paho sent already gets the old payload again.
        """
        return self._put(
            [_Message(topic, payload) for topic, payload in messages],
            PUBLICATION_QOS,
            retain=True,
            wait_for_puback=wait_for_puback,
        )

    def put_erasure(self, staged: Sequence[tuple[str, int]]) -> asyncio.Future:
        """
        Erase retained topics in stages, QoS 1, like put_retained.

        staged is (topic, stage) in the order of erasure, the stages do not decrease. A stage goes out once
        the broker confirmed everything queued before it, also after reconnect. So only an error of paho on
        the first stage reaches the caller, a later one is only logged.
        """
        return self._put(
            [_Message(topic, None, stage) for topic, stage in staged],
            PUBLICATION_QOS,
            retain=True,
            wait_for_puback=True,
        )

    # ------------------------------------------------------------
    # Own retained state in the mirror
    # ------------------------------------------------------------

    def get_own_topics_under(self, topic: str) -> list[str]:
        """
        Topics the own retained state publishes now, the topic itself and those under it.
        """
        prefix = f"{topic}/"
        return [
            own_topic
            for own_topic, mirror_record in self._mirror.items()
            if mirror_record.payload is not None and (own_topic == topic or own_topic.startswith(prefix))
        ]

    def _put_in_mirror(self, topic: str, payload: checks.Payload, qos: int, stage: int = 0) -> _MirrorRecord:
        current_record = self._mirror.get(topic)
        if current_record is None or current_record.payload is None or payload is None:
            # Appearing again and erasing go to the end. The replay keeps the order the state was built in,
            # and erases in the stages the erasure was made in.
            self._mirror.pop(topic, None)
        mirror_record = _MirrorRecord(payload, qos, stage)
        self._mirror[topic] = mirror_record
        return mirror_record

    def _forget_confirmed_erasure(self, topic: str, mirror_record: Optional[_MirrorRecord]) -> None:
        # An erasure that reached the broker needs no replay. Keeping it would grow the mirror forever.
        if (
            mirror_record is not None
            and mirror_record.payload is None
            and self._mirror.get(topic) is mirror_record
        ):
            del self._mirror[topic]

    # ------------------------------------------------------------
    # Queue of the session and sending
    # ------------------------------------------------------------

    def _put(  # pylint: disable=too-many-locals
        self,
        messages: Sequence[_Message],
        qos: int,
        retain: bool,
        wait_for_puback: bool,
    ) -> asyncio.Future:
        """
        Put messages into the mirror, when they are retained, and into the queue
        in the order given, with a barrier where the stage changes. The queue hands them to paho while
        the window has room. An error of paho on a message takes the whole call back and comes as it is.
        """
        session = self._session
        if session is None and not retain:
            raise BrokerDisconnectedError(f"broker is not connected, {messages[0].topic} is not published")

        # Messages without wait_for_puback that paho takes at once have nothing to wait for
        at_once = (
            not wait_for_puback
            and session is not None
            and not session.queue
            and session.inflight + len(messages) <= broker_connection.SEND_WINDOW
        )
        batch = None if at_once else _Batch(len(messages) if session is not None else 0)
        # The mirror record of every topic as it was before the call, for _take_back
        previous_records: list[tuple[str, Optional[_MirrorRecord]]] = []
        previous_stage = None
        for topic, payload, stage in messages:
            if payload in ("", b""):
                # An empty payload erases a retained message, the mirror keeps erasures as None
                payload = None
            mirror_record = None
            if retain:
                previous_records.append((topic, self._mirror.get(topic)))
                mirror_record = self._put_in_mirror(topic, payload, qos, stage)
            if session is None:
                continue
            if previous_stage is not None and stage != previous_stage:
                session.queue.append(_BARRIER)
            previous_stage = stage
            publication = _Publication(
                topic, payload, qos, retain=retain, mirror_record=mirror_record, batch=batch
            )
            session.queue.append(publication)
            # The send task starts at once and hands the packet to paho while the window has room
            self._send_queued(session)
            if publication.error is not None:
                self._take_back(session, previous_records, qos)
                raise publication.error
        # A future that is done already does not hold the caller
        return (batch or _Batch(0)).future

    def _take_back(
        self, session: _Session, previous_records: list[tuple[str, Optional[_MirrorRecord]]], qos: int
    ) -> None:
        """
        Undo a call whose last message failed on the way to paho. The mirror gets back what it had before
        the call. paho has sent the messages before the failed one, so their topics get the old payload again,
        or an erasure where there was none.
        """
        for topic, previous_record in previous_records:
            if previous_record is None:
                self._mirror.pop(topic, None)
            else:
                self._mirror[topic] = previous_record
        for topic, previous_record in reversed(previous_records[:-1]):
            payload = previous_record.payload if previous_record is not None else None
            session.queue.append(
                _Publication(topic, payload, qos, retain=True, mirror_record=previous_record)
            )
        self._send_queued(session)

    def _queue_replay(self, session: _Session) -> None:
        """
        Queue the own retained state in the order it was built, erasures in the stages they were made in.

        A publication that a newer change overtakes before it goes out is skipped, that change has
        a publication of its own further back in the queue.
        """
        erasure_stage = None
        for topic, mirror_record in self._mirror.items():
            # A barrier where the stage of the erasures that go in a row grows
            if mirror_record.payload is None:
                if erasure_stage is not None and mirror_record.stage > erasure_stage:
                    session.queue.append(_BARRIER)
                erasure_stage = mirror_record.stage
            else:
                erasure_stage = None
            session.queue.append(
                _Publication(
                    topic,
                    mirror_record.payload,
                    mirror_record.qos,
                    retain=True,
                    mirror_record=mirror_record,
                    replay=True,
                )
            )
        self._send_queued(session)

    def _send_queued(self, session: _Session) -> None:
        """
        Hand queued publications to the broker in order while the window has room and no barrier holds them.
        """
        if session is not self._session or self._sending_queued:
            return
        self._sending_queued = True
        try:
            queue = session.queue
            while queue:
                # A barrier lets the queue go on once everything before it is confirmed
                publication = queue[0]
                if publication is _BARRIER:
                    if session.inflight:
                        return
                    queue.popleft()
                    continue

                # A publication goes out while the window has room
                if session.inflight >= broker_connection.SEND_WINDOW:
                    return
                queue.popleft()
                session.inflight += 1
                # Started at once, so the connection gets the publications in the order of the queue
                task = asyncio.Task(self._send(session, publication), loop=session.loop, eager_start=True)
                if not task.done():
                    session.send_tasks.add(task)
                    task.add_done_callback(session.send_tasks.discard)
        finally:
            self._sending_queued = False

    async def _send(self, session: _Session, publication: _Publication) -> None:
        """
        Publish and wait for PUBACK. Retained state that timed out goes again while the link lives, unless
        the client closes.
        """
        try:
            while True:
                # A replayed publication that a newer change overtook does not go out
                if (
                    publication.replay
                    and self._mirror.get(publication.topic) is not publication.mirror_record
                ):
                    return

                try:
                    await session.connection.publish(
                        publication.topic, publication.payload, publication.qos, publication.retain
                    )
                except broker_connection.LinkError as error:
                    # An event fails, after mark_closing state fails too
                    if not publication.retain or self._closing:
                        self._end_unconfirmed(publication, str(error))
                        return
                    # The caller stops waiting at once, as it does without a link
                    publication.finish()
                    # State waits for the replay, unless the broker was only late and the change is current
                    if (
                        not isinstance(error, broker_connection.AckTimeout)
                        or session is not self._session
                        or self._mirror.get(publication.topic) is not publication.mirror_record
                    ):
                        logger.debug(
                            "Retained publish to %s left to the replay: %s", publication.topic, error
                        )
                        return
                    logger.debug("Retained publish to %s goes again: %s", publication.topic, error)
                    continue

                # Confirmed
                if publication.retain:
                    self._forget_confirmed_erasure(publication.topic, publication.mirror_record)
                return
        except asyncio.CancelledError:
            # The session ended
            self._end_unconfirmed(publication)
            raise
        except Exception as error:  # pylint: disable=broad-exception-caught
            # One broken publication must not stop the rest. It leaves the mirror, so the replay does not
            # repeat it. The caller of _put gets the error at once, and a later one fails a waiting event.
            logger.exception("Publishing to %s failed", publication.topic)
            publication.error = error
            if publication.retain and self._mirror.get(publication.topic) is publication.mirror_record:
                del self._mirror[publication.topic]
            if not publication.retain:
                publication.fail(error)
        finally:
            publication.finish()
            session.inflight -= 1
            self._send_queued(session)

    def _end_unconfirmed(self, publication: _Publication, reason: str = "the broker link is lost") -> None:
        """
        The broker did not confirm the publication in this session. State waits for the replay, an event
        fails. After mark_closing there is no replay, and everything fails.
        """
        if self._closing:
            publication.fail(ClientClosedError())
        elif publication.retain:
            publication.finish()
        else:
            publication.fail(BrokerDisconnectedError(f"{publication.topic} is not published: {reason}"))
