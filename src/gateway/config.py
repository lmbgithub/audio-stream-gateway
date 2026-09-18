"""Runtime configuration, read once from the environment."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any


class ConfigError(ValueError):
    """Raised when the environment holds a value the gateway cannot honour."""


@dataclass(frozen=True)
class Settings:
    """Everything the gateway needs to run, with defaults that work unconfigured."""

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "info"

    backend: str = "mock"

    sample_rate: int = 16000
    encoding: str = "linear16"
    max_channels: int = 1

    queue_size: int = 64

    max_session_seconds: float = 300.0
    idle_timeout_seconds: float = 30.0
    max_chunk_bytes: int = 65536

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        source = env if env is not None else dict(os.environ)

        def _str(key: str, default: str) -> str:
            return source.get(f"GATEWAY_{key}", default)

        def _int(key: str, default: int) -> int:
            raw = source.get(f"GATEWAY_{key}")
            if raw is None or raw == "":
                return default
            try:
                return int(raw)
            except ValueError as exc:
                raise ConfigError(
                    f"GATEWAY_{key} must be an integer, got {raw!r}"
                ) from exc

        def _float(key: str, default: float) -> float:
            raw = source.get(f"GATEWAY_{key}")
            if raw is None or raw == "":
                return default
            try:
                return float(raw)
            except ValueError as exc:
                raise ConfigError(f"GATEWAY_{key} must be a number, got {raw!r}") from exc

        settings = cls(
            host=_str("HOST", cls.host),
            port=_int("PORT", cls.port),
            log_level=_str("LOG_LEVEL", cls.log_level).lower(),
            backend=_str("BACKEND", cls.backend).lower(),
            sample_rate=_int("SAMPLE_RATE", cls.sample_rate),
            encoding=_str("ENCODING", cls.encoding).lower(),
            max_channels=_int("MAX_CHANNELS", cls.max_channels),
            queue_size=_int("QUEUE_SIZE", cls.queue_size),
            max_session_seconds=_float("MAX_SESSION_SECONDS", cls.max_session_seconds),
            idle_timeout_seconds=_float("IDLE_TIMEOUT_SECONDS", cls.idle_timeout_seconds),
            max_chunk_bytes=_int("MAX_CHUNK_BYTES", cls.max_chunk_bytes),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        """Fail fast at startup rather than mid-session."""
        if self.queue_size < 1:
            raise ConfigError("GATEWAY_QUEUE_SIZE must be >= 1")
        if self.sample_rate <= 0:
            raise ConfigError("GATEWAY_SAMPLE_RATE must be positive")
        if self.max_channels < 1:
            raise ConfigError("GATEWAY_MAX_CHANNELS must be >= 1")
        if self.max_chunk_bytes < 1:
            raise ConfigError("GATEWAY_MAX_CHUNK_BYTES must be >= 1")
        if self.max_session_seconds <= 0:
            raise ConfigError("GATEWAY_MAX_SESSION_SECONDS must be positive")
        if self.idle_timeout_seconds <= 0:
            raise ConfigError("GATEWAY_IDLE_TIMEOUT_SECONDS must be positive")
        if not 1 <= self.port <= 65535:
            raise ConfigError("GATEWAY_PORT must be between 1 and 65535")
