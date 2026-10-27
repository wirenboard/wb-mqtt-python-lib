"""
Broker address from a URL, a unix socket, tcp or websockets, optionally with a login.
"""

from dataclasses import dataclass, field
from typing import Literal, Optional
from urllib.parse import unquote, urlparse

from .topics import is_refused_by_broker

DEFAULT_BROKER_URL = "unix:///var/run/mosquitto/mosquitto.sock"

# How the client connects, the transports of aiomqtt
Transport = Literal["unix", "tcp", "websockets"]

# mqtt-tcp is the name wb-common takes for tcp
_TRANSPORTS: dict[str, Transport] = {"unix": "unix", "tcp": "tcp", "mqtt-tcp": "tcp", "ws": "websockets"}
# Error texts never quote the URL, it may carry the password
_INVALID_URL = "invalid MQTT broker URL, expected unix:///path, tcp://host:port or ws://host:port/path"


@dataclass(frozen=True)
class BrokerAddress:
    """
    Where and how to connect. For the unix transport the hostname is the socket path.

    websocket_path is the path of a ws:// URL, None leaves the default of paho, /mqtt. The login
    and the password are kept out of repr, so they do not reach logs and error texts.
    """

    transport: Transport
    hostname: str
    port: int = 1883
    username: Optional[str] = field(default=None, repr=False)
    password: Optional[str] = field(default=None, repr=False)
    websocket_path: Optional[str] = None


def parse_broker_url(broker_url: str) -> BrokerAddress:
    """
    Parse the broker URL.

    Args:
        broker_url: unix:///path/to/socket, tcp://host:port or ws://host:port/path, optionally with
            user:password@ before the host. mqtt-tcp:// is the same as tcp://, as in wb-common. The path
            of ws:// goes into the websocket handshake, without one it is /mqtt. Characters /, ?, #, @, :
            and % in the login or the password are percent-encoded.

    Returns:
        The broker address with the login and the password decoded.

    Raises:
        ValueError: no client could connect to this URL, for example ? or # that would cut the URL, unix://
            with a host or a login with a character the broker refuses. The text does not quote the URL.
    """
    # The URL parses and has a known scheme
    if not isinstance(broker_url, str) or "://" not in broker_url:
        raise ValueError(_INVALID_URL)
    if "?" in broker_url or "#" in broker_url:
        # Parsed, the rest of the URL would be dropped or the socket path would change
        raise ValueError(f"{_INVALID_URL}, ? and # go percent-encoded and only in the login or the password")
    try:
        parsed = urlparse(broker_url)
        port = parsed.port
        hostname = parsed.hostname
    except ValueError:
        raise ValueError(_INVALID_URL) from None
    transport = _TRANSPORTS.get(parsed.scheme)
    if transport is None:
        raise ValueError(f"{_INVALID_URL}, the scheme is not unix, tcp, mqtt-tcp or ws")

    # The login. mosquitto drops the connection on a login with a character it refuses, and the client
    # would retry forever. A password is binary data in MQTT, any character goes.
    username = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    if username is not None and any(is_refused_by_broker(char) for char in username):
        raise ValueError(f"{_INVALID_URL}, the login has a character the broker refuses")

    # A unix socket path, or a host with a port
    if parsed.scheme == "unix":
        if not parsed.path:
            raise ValueError(f"{_INVALID_URL}, no socket path")
        if hostname:
            raise ValueError(f"{_INVALID_URL}, a unix socket path goes after three slashes")
        return BrokerAddress(transport, parsed.path, username=username, password=password)
    if hostname is None:
        raise ValueError(f"{_INVALID_URL}, no host")
    if port is None:
        raise ValueError(f"{_INVALID_URL}, no port")
    websocket_path = (parsed.path or None) if transport == "websockets" else None
    return BrokerAddress(transport, hostname, port, username, password, websocket_path)
