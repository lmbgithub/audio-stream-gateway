"""Session lifecycle, backpressure, and the state machine.

Deliberately transport-agnostic: this module knows nothing about WebSockets. It
consumes frames and produces frames, which is what makes the interesting
behaviour — the state machine, the drop policy, the timeout paths — testable
without a socket or an event-loop server.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import deque
from enum import Enum
from typing import Any, AsyncIterator

from gateway.backends.base import ASRBackend, AudioFormat, BackendError
from gateway.config import Settings
from gateway.metrics import Metrics, SessionTimer
from gateway.protocol import (
    ClientMessage,
    ErrorCode,
    ProtocolError,
    ServerMessage,
    StartRequest,
    decode_client_frame,
    transcript_frame,
)


class SessionState(str, Enum):
    NEW = "new"
    RUNNING = "running"
    STOPPING = "stopping"
    CLOSED = "closed"


class AudioBuffer:
    """A bounded queue that drops the *oldest* chunk when full.

    The usual bounded-queue reflex is to block the producer or drop the newest
    arrival. Both are wrong for live audio. Blocking propagates backpressure to a
    client that cannot slow down a speaking human, and dropping the newest keeps
    the stalest audio in the buffer — maximising the latency the user actually
    perceives. Dropping the oldest keeps the transcript close to the present,
    which is the property a real-time system is built to protect.

    Every drop is counted and surfaced. Silent data loss is the failure mode that
    turns into an unreproducible accuracy complaint three weeks later.
    """

    def __init__(self, maxsize: int = 64) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self.maxsize = maxsize
        self.dropped = 0
        self._items: deque[bytes] = deque()
        self._closed = False
        self._event = asyncio.Event()

    def push(self, chunk: bytes) -> bool:
        """Append a chunk. Returns False if an older chunk had to be discarded."""
        if self._closed:
            raise RuntimeError("cannot push to a closed buffer")
        dropped = False
        if len(self._items) >= self.maxsize:
            self._items.popleft()
            self.dropped += 1
            dropped = True
        self._items.append(chunk)
        self._event.set()
        return not dropped

    def close(self) -> None:
        """Signal end of stream; the iterator drains and then stops."""
        self._closed = True
        self._event.set()

    @property
    def closed(self) -> bool:
        return self._closed

    def __len__(self) -> int:
        return len(self._items)

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            while not self._items and not self._closed:
                self._event.clear()
                await self._event.wait()
            if self._items:
                yield self._items.popleft()
            elif self._closed:
                return


class StreamSession:
    """One client streaming session, from `start` to `closed`."""

    def __init__(
        self,
        backend: ASRBackend,
        settings: Settings,
        metrics: Metrics | None = None,
        *,
        session_id: str | None = None,
    ) -> None:
        self.id = session_id or uuid.uuid4().hex[:16]
        self.backend = backend
        self.settings = settings
        self.metrics = metrics or Metrics()
        self.state = SessionState.NEW
        self.timer = SessionTimer()
        self.audio: AudioFormat | None = None
        self.buffer = AudioBuffer(settings.queue_size)
        self._seq = 0

    # -- frame helpers ----------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _frame(self, kind: ServerMessage, **fields: Any) -> dict[str, Any]:
        return {
            "type": kind.value,
            "session_id": self.id,
            "seq": self._next_seq(),
            **fields,
        }

    # -- lifecycle --------------------------------------------------------

    def handle_text(self, raw: str) -> dict[str, Any] | None:
        """Handle a JSON control frame, returning a response frame if any."""
        message, payload = decode_client_frame(raw)

        if message is ClientMessage.PING:
            return self._frame(ServerMessage.PONG)

        if message is ClientMessage.START:
            return self._start(payload)

        if message is ClientMessage.STOP:
            return self._stop()

        raise ProtocolError(ErrorCode.UNKNOWN_TYPE, f"unhandled message: {message}")

    def _start(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.state is not SessionState.NEW:
            raise ProtocolError(
                ErrorCode.INVALID_STATE,
                f"'start' is only valid on a new session (state: {self.state.value})",
            )

        request = StartRequest.parse(payload)
        audio = AudioFormat(
            sample_rate=request.sample_rate,
            encoding=request.encoding,
            channels=request.channels,
            language=request.language,
        )

        # Validate against both the deployment's contract and the backend's own
        # capability. The two are different constraints and a mismatch in either
        # one produces garbage transcripts rather than an error, so both are
        # checked before a single byte of audio is accepted.
        if audio.channels > self.settings.max_channels:
            raise ProtocolError(
                ErrorCode.UNSUPPORTED_AUDIO,
                f"{audio.channels} channels requested, this deployment accepts at most "
                f"{self.settings.max_channels}",
            )
        if not self.backend.supports(audio):
            raise ProtocolError(
                ErrorCode.UNSUPPORTED_AUDIO,
                f"backend {self.backend.name!r} does not support encoding {audio.encoding!r}",
            )

        self.audio = audio
        self.state = SessionState.RUNNING
        self.metrics.increment("sessions_started")

        return self._frame(
            ServerMessage.READY,
            backend=self.backend.name,
            sample_rate=audio.sample_rate,
            encoding=audio.encoding,
            channels=audio.channels,
            language=audio.language,
            interim_results=request.interim_results,
        )

    def _stop(self) -> dict[str, Any] | None:
        if self.state is SessionState.RUNNING:
            self.state = SessionState.STOPPING
            self.buffer.close()
            return None
        if self.state in (SessionState.STOPPING, SessionState.CLOSED):
            return None
        raise ProtocolError(
            ErrorCode.INVALID_STATE, "'stop' received before 'start'"
        )

    def handle_audio(self, chunk: bytes) -> None:
        """Accept one binary audio frame."""
        if self.state is not SessionState.RUNNING:
            raise ProtocolError(
                ErrorCode.INVALID_STATE,
                f"audio received while session is {self.state.value}; send 'start' first",
            )
        if len(chunk) > self.settings.max_chunk_bytes:
            raise ProtocolError(
                ErrorCode.CHUNK_TOO_LARGE,
                f"chunk of {len(chunk)} bytes exceeds the "
                f"{self.settings.max_chunk_bytes}-byte limit",
            )

        accepted = self.buffer.push(chunk)
        self.timer.record_audio(len(chunk))
        self.metrics.increment("audio_chunks_received")
        self.metrics.increment("audio_bytes_received", len(chunk))
        if not accepted:
            self.timer.record_drop()
            self.metrics.increment("audio_chunks_dropped")

    async def transcripts(self) -> AsyncIterator[dict[str, Any]]:
        """Drive the backend and yield server frames until the stream ends."""
        if self.state is SessionState.NEW or self.audio is None:
            raise ProtocolError(ErrorCode.INVALID_STATE, "session has not started")

        try:
            async for result in self.backend.stream(self.audio, self.buffer.__aiter__()):
                self.timer.record_transcript(is_final=result.is_final)
                if self.timer.partials_sent + self.timer.finals_sent == 1:
                    ttfb = self.timer.ttfb_ms
                    if ttfb is not None:
                        self.metrics.observe("time_to_first_transcript", ttfb)
                self.metrics.increment(
                    "transcripts_final" if result.is_final else "transcripts_partial"
                )
                yield transcript_frame(
                    session_id=self.id,
                    seq=self._next_seq(),
                    text=result.text,
                    is_final=result.is_final,
                    start_ms=result.start_ms,
                    end_ms=result.end_ms,
                    confidence=result.confidence,
                )
        except BackendError as exc:
            # A vendor failure is translated at the boundary. Letting a backend
            # exception reach the client would leak implementation detail and
            # give the client nothing it can branch on.
            self.metrics.increment("backend_errors")
            raise ProtocolError(ErrorCode.BACKEND_ERROR, str(exc)) from exc

    def close(self) -> dict[str, Any]:
        """Finalize the session and return the closing frame with its metrics."""
        if self.state is SessionState.CLOSED:
            return self._frame(ServerMessage.CLOSED, **self.timer.summary())

        self.state = SessionState.CLOSED
        if not self.buffer.closed:
            self.buffer.close()
        self.timer.close()

        self.metrics.increment("sessions_closed")
        self.metrics.observe("session_duration", self.timer.duration_ms)
        if self.timer.chunks_dropped:
            self.metrics.increment("sessions_with_drops")

        summary = self.timer.summary()
        summary["buffer_capacity"] = self.buffer.maxsize
        return self._frame(ServerMessage.CLOSED, **summary)

    def metrics_frame(self) -> dict[str, Any]:
        """A snapshot frame a client can request mid-session."""
        return self._frame(ServerMessage.METRICS, **self.timer.summary())


async def drain_with_timeout(session: StreamSession, timeout: float) -> None:
    """Close the buffer once `timeout` seconds pass with no new audio.

    Runs alongside the reader so a client that opens a socket, sends `start`, and
    then disappears does not hold a worker slot until the process restarts.
    """
    last_seen = session.timer.chunks_received
    idle = 0.0
    step = 0.05
    while session.state is SessionState.RUNNING:
        await asyncio.sleep(step)
        if session.timer.chunks_received != last_seen:
            last_seen = session.timer.chunks_received
            idle = 0.0
            continue
        idle += step
        if idle >= timeout:
            session.metrics.increment("sessions_idle_timeout")
            session.buffer.close()
            return
