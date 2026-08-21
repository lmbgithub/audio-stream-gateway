"""The backend contract.

The gateway owns transport, session lifecycle, backpressure and metrics; a
backend owns nothing but turning audio into transcripts. Keeping that line sharp
is what makes a vendor swap a one-file change instead of a rewrite, and it is
what lets the whole system be tested without credentials or network access.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Protocol, runtime_checkable


class BackendError(RuntimeError):
    """Raised when a backend cannot fulfil a request.

    The gateway translates this into a protocol error frame rather than letting
    a vendor-specific exception escape to the client.
    """


@dataclass(frozen=True)
class AudioFormat:
    """The negotiated audio contract for one session."""

    sample_rate: int
    encoding: str
    channels: int = 1
    language: str = "en"

    @property
    def bytes_per_second(self) -> int:
        """Only meaningful for uncompressed PCM; used for pacing, not billing."""
        width = 2 if self.encoding == "linear16" else 1
        return self.sample_rate * width * self.channels


@dataclass(frozen=True)
class Transcript:
    """One transcript event emitted by a backend."""

    text: str
    is_final: bool
    start_ms: int | None = None
    end_ms: int | None = None
    confidence: float | None = None
    metadata: dict[str, Any] | None = None


@runtime_checkable
class ASRBackend(Protocol):
    """Structural interface every backend implements.

    `stream` consumes an async iterator of audio chunks and yields transcripts as
    they become available. It must not buffer the entire stream: the point of the
    interface is that the first partial can be emitted before the last chunk
    arrives.
    """

    name: str

    def supports(self, audio: AudioFormat) -> bool:
        """Return True when the backend can handle this audio contract."""
        ...

    async def stream(
        self, audio: AudioFormat, chunks: AsyncIterator[bytes]
    ) -> AsyncIterator[Transcript]:
        """Yield transcripts for the incoming audio stream."""
        ...
