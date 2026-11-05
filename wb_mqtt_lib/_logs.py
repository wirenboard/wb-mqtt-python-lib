"""
Loggers of the library modules and how failures of service code are logged.
"""

import logging

from ._errors import BrokerDisconnectedError, ControlMissingError, ReadonlyControlError

# Normal operation while the broker or a driver restarts, or when a driver makes a control readonly.
# Not a bug in the service
_EXPECTED_FAILURES = (BrokerDisconnectedError, ControlMissingError, ReadonlyControlError)


def get_logger(module_name: str) -> logging.Logger:
    """
    Logger of a library module, named by its path without the underscores of the internal packages.

    wb.<service>.wb_mqtt_lib._mqtt.subscriber logs as wb.<service>.wb_mqtt_lib.mqtt.subscriber.
    """
    return logging.getLogger(module_name.replace("._", "."))


def log_failure(logger: logging.Logger, description: str, error: BaseException) -> None:
    """
    Log an exception that service code raised in a handler or a background task.

    A lost broker link, a control that is gone during a driver restart or a command to a readonly control
    is one WARNING line, anything else is an ERROR with the traceback.
    """
    if isinstance(error, _EXPECTED_FAILURES):
        logger.warning("%s failed: %s", description, error)
    else:
        logger.error("%s failed", description, exc_info=error)
