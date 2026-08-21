"""Real-time WebSocket gateway for streaming speech recognition."""

from gateway.config import ConfigError, Settings
from gateway.metrics import Distribution, Metrics, SessionTimer
from gateway.protocol import ErrorCode, ProtocolError, ServerMessage, StartRequest
from gateway.session import AudioBuffer, SessionState, StreamSession

__version__ = "0.1.0"

__all__ = [
    "ConfigError",
    "Settings",
    "Distribution",
    "Metrics",
    "SessionTimer",
    "ErrorCode",
    "ProtocolError",
    "ServerMessage",
    "StartRequest",
    "AudioBuffer",
    "SessionState",
    "StreamSession",
    "__version__",
]
