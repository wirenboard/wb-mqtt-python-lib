"""
Background tasks the library keeps a reference to, and how failures of service code are logged.
"""

import asyncio
import logging
from typing import Any, Coroutine, Iterable, Optional

from .errors import BrokerDisconnectedError, ControlMissingError

# Normal operation while the broker or a driver restarts, not a bug in the service
_EXPECTED_FAILURES = (BrokerDisconnectedError, ControlMissingError)


def is_cancelled_from_outside() -> bool:
    """
    Whether the current task was cancelled, not just handed a CancelledError by something it awaited.
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def log_failure(logger: logging.Logger, description: str, error: BaseException) -> None:
    """
    Log an exception that service code raised in a handler or a background task.

    A lost broker link or a control that is gone during a driver restart is one WARNING line,
    anything else is an ERROR with the traceback.
    """
    if isinstance(error, _EXPECTED_FAILURES):
        logger.warning("%s failed: %s", description, error)
    else:
        logger.error("%s failed", description, exc_info=error)


def retrieve_error(future: asyncio.Future) -> None:
    """
    Mark the error of a future as read, for a future nobody may wait for any more.
    """
    if not future.cancelled():
        future.exception()


class TaskSet:
    """
    Tasks that run in the background until they end or the owner cancels them.

    The set keeps a reference to every task, otherwise Python may collect a task in the middle of its work,
    and logs the error of a task that failed.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._tasks: set[asyncio.Task] = set()

    def __len__(self) -> int:
        return len(self._tasks)

    def __contains__(self, task: object) -> bool:
        return task in self._tasks

    def get_tasks_with_prefix(self, prefix: str) -> list[asyncio.Task]:
        """
        The running tasks whose name starts with prefix.
        """
        return [task for task in self._tasks if task.get_name().startswith(prefix)]

    def start_task(
        self, coroutine: Coroutine[Any, Any, Any], name: str, log_error: bool = True
    ) -> asyncio.Task:
        """
        Start a task and keep a reference to it.

        Args:
            log_error: log the error of the task. False for a task whose caller gets the error itself.
        """
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._forget_and_log_failure if log_error else self._forget_silently)
        return task

    async def cancel_all_tasks(
        self, extra_tasks: Iterable[asyncio.Task] = (), spare: Optional[asyncio.Task] = None
    ) -> None:
        """
        Cancel every task and extra_tasks and wait until they end.

        The task that calls this and spare are skipped, so close() works from a background task of the client.
        """
        current_task = asyncio.current_task()
        tasks = [
            task for task in (*self._tasks, *extra_tasks) if task is not current_task and task is not spare
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _forget_silently(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        retrieve_error(task)

    def _forget_and_log_failure(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            if not task.cancelling():
                # Nobody cancelled the task, it ended with a CancelledError of something it awaited
                log_failure(self._logger, f"Task {task.get_name()}", asyncio.CancelledError())
        elif task.exception() is not None:
            log_failure(self._logger, f"Task {task.get_name()}", task.exception())
