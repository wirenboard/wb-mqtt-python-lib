"""
Publication and the own retained state that survives a lost broker link.

Own retained state lives in the mirror, what should lie in the broker. A change goes into the mirror at once
and into the queue of the session in the order of the calls, the queue hands it to the broker. So a cancelled
call never leaves a change half sent, and a change made later never goes out before an earlier one.
After reconnect a new session queues the whole mirror again, the broker on the controller keeps nothing.
"""

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Optional, Sequence

from . import link, topics
from .errors import BrokerDisconnectedError, ClientClosedError
from .tasks import retrieve_error

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MirrorEntry:
    payload: link.Payload
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
    payload: link.Payload
    qos: int
    retain: bool
    # The mirror entry a retained publication carries. One whose entry is no longer the current one
    # of its topic is out of date, a newer change has a publication of its own further back in the queue.
    entry: Optional[_MirrorEntry] = None
    batch: Optional[_Batch] = None
    replay: bool = False
    finished: bool = False

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

    def __init__(self, connection: link.Connection) -> None:
        self.connection = connection
        self.loop = asyncio.get_running_loop()
        self.outbox: deque[Optional[_Publication]] = deque()
        self.inflight = 0
        self.send_tasks: set[asyncio.Task] = set()


class Publisher:
    """
    Publishes messages and the own retained state, also again after a lost link.
    """

    def __init__(self) -> None:
        self._session: Optional[_Session] = None
        self._mirror: dict[str, _MirrorEntry] = {}
        # Send tasks of the session that ended, cancelled and not finished yet
        self._ended_session_tasks: list[asyncio.Task] = []
        self._sending_queued = False
        self._closing = False

    @property
    def mirror_size(self) -> int:
        return len(self._mirror)

    def mark_closing(self) -> None:
        """
        The client is closing, there is no replay after it. Publications the broker has not confirmed
        by the end of the session fail with ClientClosedError, retained ones too.
        """
        self._closing = True

    def connection_restored(self, connection: link.Connection) -> None:
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
        for publication in session.outbox:
            if publication is not _BARRIER:
                self._drop(publication)
        session.outbox.clear()
        for task in session.send_tasks:
            task.cancel()
        self._ended_session_tasks = list(session.send_tasks)

    async def wait_ended_session(self) -> None:
        """
        Wait until the send tasks of the session that ended are gone.
        """
        ended, self._ended_session_tasks = self._ended_session_tasks, []
        await asyncio.gather(*ended, return_exceptions=True)

    def retained_under(self, topic: str) -> list[str]:
        """
        The topic and the topics under it that the own retained state publishes now.
        """
        prefix = f"{topic}/"
        return [
            mirrored
            for mirrored, entry in self._mirror.items()
            if entry.payload is not None and (mirrored == topic or mirrored.startswith(prefix))
        ]

    async def publish(  # pylint: disable=too-many-arguments
        self,
        topic: str,
        payload: link.Payload = None,
        qos: int = 1,
        retain: bool = False,
        *,
        wait_for_puback: bool = True,
    ) -> None:
        """
        Publish a message and return once the broker confirmed it.

        A retained message is state, without a link it waits in the mirror and goes out after reconnect.
        When the broker does not confirm it in link.ACK_TIMEOUT_S while the link lives, the call returns
        as well and the message goes again in its place. A non-retained message is an event, without a link
        it is an error. A cancelled call does not take the message back.

        Without wait_for_puback a retained message returns once the connection took it, a send task waits for
        PUBACK. A full window or a queue still holds the caller until the broker confirms, so a service cannot
        run ahead of the broker. An event always waits, its caller learns whether it went out.

        Raises:
            ValueError: the topic or the QoS is invalid.
            TypeError: the payload is a bool or not str, bytes, int, float or None.
            BrokerDisconnectedError: an event while the link is down, or the link is lost or the broker
                does not confirm it in link.ACK_TIMEOUT_S.
            ClientClosedError: the client closes before the broker confirmed the message.
        """
        if not isinstance(topic, topics.CheckedTopic):
            topics.check_topic(topic)
        link.check_qos(qos)
        link.check_payload(payload)
        if payload in ("", b""):
            # An empty payload erases a retained message, the mirror keeps erasures as None
            payload = None
        elif isinstance(payload, bytearray):
            # A buffer the service changes later must not change what goes out after reconnect
            payload = bytes(payload)
        session = self._session
        entry = None
        if retain:
            entry = self._put_in_mirror(topic, payload, qos)
            if session is None:
                return
        elif session is None:
            raise BrokerDisconnectedError(f"broker is not connected, {topic} is not published")
        if retain and not wait_for_puback and not session.outbox and session.inflight < link.SEND_WINDOW:
            # The send task starts at once, the packet reaches paho before the call returns
            session.outbox.append(_Publication(topic, payload, qos, retain, entry))
            self._send_queued(session)
            return
        batch = _Batch(1)
        session.outbox.append(_Publication(topic, payload, qos, retain, entry, batch))
        self._send_queued(session)
        await asyncio.shield(batch.future)

    def queue_retained(self, messages: Sequence[tuple[str, link.Payload]]) -> asyncio.Future:
        """
        Put own retained messages into the mirror and the queue together, in the order given, QoS 1.

        Returns:
            A future that ends when the broker confirmed every message or they are left to the replay.
            It fails with ClientClosedError when the client closes before that.
        """
        return self._queue_staged([(topic, payload, 0) for topic, payload in messages])

    def queue_erasure(self, staged: Sequence[tuple[str, int]]) -> asyncio.Future:
        """
        Erase retained topics in stages, QoS 1.

        staged is (topic, stage) in the order of erasure, the stages do not decrease. A stage goes out once
        the broker confirmed everything queued before it, also after reconnect.

        Returns:
            A future that ends when the broker confirmed every erasure or they are left to the replay.
            It fails with ClientClosedError when the client closes before that.
        """
        return self._queue_staged([(topic, None, stage) for topic, stage in staged])

    def _queue_staged(self, staged: Sequence[tuple[str, link.Payload, int]]) -> asyncio.Future:
        """
        Put retained messages into the mirror and the queue in the order given, QoS 1, with a barrier
        where the stage changes.
        """
        session = self._session
        batch = _Batch(len(staged) if session is not None else 0)
        previous = None
        for topic, payload, stage in staged:
            entry = self._put_in_mirror(topic, payload, 1, stage)
            if session is None:
                continue
            if previous is not None and stage != previous:
                session.outbox.append(_BARRIER)
            previous = stage
            session.outbox.append(_Publication(topic, payload, 1, True, entry, batch))
        if session is not None:
            self._send_queued(session)
        return batch.future

    def _queue_replay(self, session: _Session) -> None:
        """
        Queue the own retained state in the order it was built, erasures in the stages they were made in.

        A publication that a newer change overtakes before it goes out is skipped, that change has
        a publication of its own further back in the queue.
        """
        stage = None
        for topic, entry in self._mirror.items():
            if entry.payload is None:
                if stage is not None and entry.stage > stage:
                    session.outbox.append(_BARRIER)
                stage = entry.stage
            else:
                stage = None
            session.outbox.append(_Publication(topic, entry.payload, entry.qos, True, entry, replay=True))
        self._send_queued(session)

    def _send_queued(self, session: _Session) -> None:
        """
        Hand queued publications to the broker in order while the window has room and no barrier holds them.
        """
        if session is not self._session or self._sending_queued:
            return
        self._sending_queued = True
        try:
            outbox = session.outbox
            while outbox:
                publication = outbox[0]
                if publication is _BARRIER:
                    if session.inflight:
                        return
                    outbox.popleft()
                    continue
                if session.inflight >= link.SEND_WINDOW:
                    return
                outbox.popleft()
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
                if publication.replay and self._mirror.get(publication.topic) is not publication.entry:
                    return
                try:
                    await session.connection.publish(
                        publication.topic, publication.payload, publication.qos, publication.retain
                    )
                except link.LinkError as error:
                    if not publication.retain or self._closing:
                        self._drop(publication, str(error))
                        return
                    # The caller stops waiting at once, as it does without a link
                    publication.finish()
                    if (
                        not isinstance(error, link.AckTimeout)
                        or session is not self._session
                        or self._mirror.get(publication.topic) is not publication.entry
                    ):
                        logger.debug(
                            "Retained publish to %s left to the replay: %s", publication.topic, error
                        )
                        return
                    logger.debug("Retained publish to %s goes again: %s", publication.topic, error)
                    continue
                if publication.retain:
                    self._forget_confirmed_erasure(publication.topic, publication.entry)
                return
        except asyncio.CancelledError:
            # The session ended
            self._drop(publication)
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            # One broken publication must not stop the rest of the own state
            logger.exception("Own retained message on %s dropped", publication.topic)
            if publication.retain and self._mirror.get(publication.topic) is publication.entry:
                del self._mirror[publication.topic]
            self._drop(publication)
        finally:
            publication.finish()
            session.inflight -= 1
            self._send_queued(session)

    def _drop(self, publication: _Publication, reason: str = "the broker link is lost") -> None:
        """
        The broker did not confirm the publication in this session. State waits for the replay, an event
        fails. After mark_closing there is no replay, and everything fails.
        """
        if self._closing:
            publication.fail(ClientClosedError("the client is closed"))
        elif publication.retain:
            publication.finish()
        else:
            publication.fail(BrokerDisconnectedError(f"{publication.topic} is not published: {reason}"))

    def _put_in_mirror(self, topic: str, payload: link.Payload, qos: int, stage: int = 0) -> _MirrorEntry:
        current = self._mirror.get(topic)
        if current is None or current.payload is None or payload is None:
            # Appearing again and erasing go to the end. The replay keeps the order the state was built in,
            # and erases in the stages the erasure was made in.
            self._mirror.pop(topic, None)
        entry = _MirrorEntry(payload, qos, stage)
        self._mirror[topic] = entry
        return entry

    def _forget_confirmed_erasure(self, topic: str, entry: Optional[_MirrorEntry]) -> None:
        # An erasure that reached the broker needs no replay. Keeping it would grow the mirror forever.
        if entry is not None and entry.payload is None and self._mirror.get(topic) is entry:
            del self._mirror[topic]
