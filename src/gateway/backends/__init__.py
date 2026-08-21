"""Backend registry.

Backends register by name so `GATEWAY_BACKEND` selects one without the app
module importing every vendor SDK at startup.
"""

from __future__ import annotations

from typing import Callable

from gateway.backends.base import ASRBackend, AudioFormat, BackendError, Transcript
from gateway.backends.mock import EchoBackend, MockBackend

_REGISTRY: dict[str, Callable[[], ASRBackend]] = {
    "mock": MockBackend,
    "echo": EchoBackend,
}


def register(name: str, factory: Callable[[], ASRBackend]) -> None:
    """Register a backend factory under `name`."""
    _REGISTRY[name.lower()] = factory


def available() -> list[str]:
    return sorted(_REGISTRY)


def create(name: str) -> ASRBackend:
    """Instantiate a registered backend by name."""
    factory = _REGISTRY.get(name.lower())
    if factory is None:
        raise BackendError(
            f"unknown backend {name!r}; available: {', '.join(available())}"
        )
    return factory()


__all__ = [
    "ASRBackend",
    "AudioFormat",
    "BackendError",
    "Transcript",
    "MockBackend",
    "EchoBackend",
    "available",
    "create",
    "register",
]
