"""
Loggers of the library modules and how failures of service code are logged.
"""

import logging

from ._errors import BrokerDisconnectedError, ControlMissingError

# Normal operation while the broker or a driver restarts, not a bug in the service
_EXPECTED_FAILURES = (BrokerDisconnectedError, ControlMissingError)


def get_logger(module_name: str) -> logging.Logger:
    """
    Logger of a library module, named by its path without the underscores of the internal packages.

    wb.<service>.wb_mqtt_lib._mqtt.subscriber logs as wb.<service>.wb_mqtt_lib.mqtt.subscriber.
    """
    return logging.getLogger(module_name.replace("._", "."))


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
