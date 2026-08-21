"""Wire protocol for the streaming session.

Control frames are JSON text, audio frames are binary. Keeping audio off the
JSON path avoids base64 on the hot path: a 20 ms PCM chunk is 640 bytes, and
base64 would add a third to that on every frame for no benefit.

Every server frame carries the session id and a monotonically increasing `seq`,
so a client can detect gaps and order frames without relying on delivery order.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ClientMessage(str, Enum):
    START = "start"
    STOP = "stop"
    PING = "ping"


class ServerMessage(str, Enum):
    READY = "ready"
    PARTIAL = "partial"
    FINAL = "final"
    METRICS = "metrics"
    PONG = "pong"
    ERROR = "error"
    CLOSED = "closed"


class ErrorCode(str, Enum):
    """Closed set, so clients can branch on the code and not on prose."""

    INVALID_JSON = "invalid_json"
    UNKNOWN_TYPE = "unknown_type"
    INVALID_STATE = "invalid_state"
    UNSUPPORTED_AUDIO = "unsupported_audio"
    CHUNK_TOO_LARGE = "chunk_too_large"
    SESSION_TIMEOUT = "session_timeout"
    IDLE_TIMEOUT = "idle_timeout"
    BACKEND_ERROR = "backend_error"


class ProtocolError(Exception):
    """Raised when a client frame cannot be honoured."""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def to_frame(self, session_id: str = "", seq: int = 0) -> dict[str, Any]:
        return {
            "type": ServerMessage.ERROR.value,
            "session_id": session_id,
            "seq": seq,
            "code": self.code.value,
            "message": self.message,
        }


@dataclass(frozen=True)
class StartRequest:
    """Audio contract proposed by the client at the start of a session."""

    sample_rate: int
    encoding: str
    channels: int = 1
    language: str = "en"
    interim_results: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, payload: dict[str, Any]) -> "StartRequest":
        try:
            sample_rate = int(payload.get("sample_rate", 16000))
            channels = int(payload.get("channels", 1))
        except (TypeError, ValueError) as exc:
            raise ProtocolError(
                ErrorCode.UNSUPPORTED_AUDIO, "sample_rate and channels must be integers"
            ) from exc

        encoding = str(payload.get("encoding", "linear16")).lower()
        known = {"sample_rate", "encoding", "channels", "language", "interim_results"}
        return cls(
            sample_rate=sample_rate,
            encoding=encoding,
            channels=channels,
            language=str(payload.get("language", "en")),
            interim_results=bool(payload.get("interim_results", True)),
            metadata={k: v for k, v in payload.items() if k not in known},
        )


def decode_client_frame(raw: str) -> tuple[ClientMessage, dict[str, Any]]:
    """Decode a JSON control frame into a known message type and its payload."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProtocolError(ErrorCode.INVALID_JSON, f"invalid JSON: {exc.msg}") from exc

    if not isinstance(payload, dict):
        raise ProtocolError(ErrorCode.INVALID_JSON, "expected a JSON object")

    raw_type = payload.get("type")
    if raw_type is None:
        raise ProtocolError(ErrorCode.UNKNOWN_TYPE, "missing 'type' field")

    try:
        message = ClientMessage(str(raw_type).lower())
    except ValueError as exc:
        allowed = ", ".join(m.value for m in ClientMessage)
        raise ProtocolError(
            ErrorCode.UNKNOWN_TYPE, f"unknown message type {raw_type!r}; expected one of: {allowed}"
        ) from exc

    return message, payload


def transcript_frame(
    *,
    session_id: str,
    seq: int,
    text: str,
    is_final: bool,
    start_ms: int | None = None,
    end_ms: int | None = None,
    confidence: float | None = None,
) -> dict[str, Any]:
    """Build a `partial` or `final` transcript frame."""
    frame: dict[str, Any] = {
        "type": (ServerMessage.FINAL if is_final else ServerMessage.PARTIAL).value,
        "session_id": session_id,
        "seq": seq,
        "text": text,
        "is_final": is_final,
    }
    if start_ms is not None:
        frame["start_ms"] = start_ms
    if end_ms is not None:
        frame["end_ms"] = end_ms
    if confidence is not None:
        frame["confidence"] = round(float(confidence), 4)
    return frame
