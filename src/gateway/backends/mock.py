"""A deterministic backend that needs no credentials.

This exists so the gateway is runnable and testable the moment it is cloned. It
also pins down the streaming contract: partials grow word by word and a final is
emitted at the end of each phrase, which is the shape a real streaming ASR
produces and the shape client code has to handle.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

from gateway.backends.base import AudioFormat, BackendError, Transcript

DEFAULT_PHRASES: tuple[str, ...] = (
    "the quick brown fox",
    "jumps over the lazy dog",
    "and keeps on running",
)

SUPPORTED_ENCODINGS = frozenset({"linear16", "mulaw", "flac"})


class MockBackend:
    """Emits scripted transcripts paced by the audio actually received."""

    name = "mock"

    def __init__(
        self,
        phrases: tuple[str, ...] = DEFAULT_PHRASES,
        *,
        chunks_per_word: int = 1,
        emit_delay: float = 0.0,
        fail_after: int | None = None,
    ) -> None:
        self.phrases = phrases
        self.chunks_per_word = max(1, chunks_per_word)
        self.emit_delay = emit_delay
        # `fail_after` makes the gateway's error path testable without needing a
        # real backend to misbehave on demand.
        self.fail_after = fail_after

    def supports(self, audio: AudioFormat) -> bool:
        return audio.encoding in SUPPORTED_ENCODINGS and audio.channels >= 1

    async def stream(
        self, audio: AudioFormat, chunks: AsyncIterator[bytes]
    ) -> AsyncIterator[Transcript]:
        if not self.supports(audio):
            raise BackendError(f"unsupported audio format: {audio.encoding}")

        words = [w for phrase in self.phrases for w in phrase.split()]
        phrase_ends = set()
        cursor = 0
        for phrase in self.phrases:
            cursor += len(phrase.split())
            phrase_ends.add(cursor)

        received = 0
        word_index = 0
        current: list[str] = []
        elapsed_ms = 0

        async for chunk in chunks:
            received += 1
            if self.fail_after is not None and received > self.fail_after:
                raise BackendError("simulated backend failure")

            # Advance the transcript in proportion to the audio actually seen,
            # so latency measured against this backend reflects the gateway's
            # own overhead rather than an arbitrary sleep.
            elapsed_ms += int(len(chunk) / max(1, audio.bytes_per_second) * 1000)
            if received % self.chunks_per_word:
                continue
            if word_index >= len(words):
                continue

            current.append(words[word_index])
            word_index += 1
            if self.emit_delay:
                await asyncio.sleep(self.emit_delay)

            is_final = word_index in phrase_ends
            yield Transcript(
                text=" ".join(current),
                is_final=is_final,
                start_ms=max(0, elapsed_ms - 200),
                end_ms=elapsed_ms,
                confidence=0.95 if is_final else 0.72,
            )
            if is_final:
                current = []

        # Flush whatever was accumulated but never closed by a phrase boundary.
        # Dropping it would lose the tail of every stream that ends mid-phrase.
        if current:
            yield Transcript(
                text=" ".join(current),
                is_final=True,
                start_ms=max(0, elapsed_ms - 200),
                end_ms=elapsed_ms,
                confidence=0.90,
            )


class EchoBackend:
    """Returns the byte count of each chunk. Useful for transport-level debugging."""

    name = "echo"

    def supports(self, audio: AudioFormat) -> bool:
        return True

    async def stream(
        self, audio: AudioFormat, chunks: AsyncIterator[bytes]
    ) -> AsyncIterator[Transcript]:
        total = 0
        async for chunk in chunks:
            total += len(chunk)
            yield Transcript(text=f"received {total} bytes", is_final=False)
        yield Transcript(text=f"received {total} bytes", is_final=True)
