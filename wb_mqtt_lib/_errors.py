"""
Errors the library raises. aiomqtt errors never reach the service.
"""


class MqttLibError(Exception):
    """
    Base class of the library errors.
    """


class BrokerDisconnectedError(MqttLibError):
    """
    No broker link for what cannot wait for it.

    That is an event, such as a command or a pushbutton press, and a read of what the broker keeps.
    A broker that does not confirm an event in 5 s or answer a read in 30 s counts as a lost link.
    """


class NotOwnError(MqttLibError):
    """
    An operation allowed only on devices this client created.
    """


class ReadonlyControlError(MqttLibError):
    """
    A command to a foreign readonly control. By the convention such a control has no /on.
    """


class ControlMissingError(MqttLibError):
    """
    The control is not published right now, for example while its driver restarts, or an action
    of the owner on an own device the service removed.
    """


class LoginRejectedError(MqttLibError):
    """
    The broker rejected the login at the first connection. Retrying will not help.
    """


class ClientClosedError(MqttLibError):
    """
    The client is closed. Fields still read, writing, waiting and new listeners do not work.
    """


class DeviceTakenError(MqttLibError, ValueError):
    """
    The name is taken by a device of another driver, a device without a driver or controls alone,
    the client does not publish over it.
    """


class AlreadyCreatedError(MqttLibError, ValueError):
    """
    The device or the control was created by this client already.
    """


class InvalidNameError(MqttLibError, ValueError):
    """
    A device, control or driver name, or a control address, that would break the topics or the connection.
    """
