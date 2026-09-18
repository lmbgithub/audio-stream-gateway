"""ASGI application: health, metrics, and the streaming WebSocket endpoint.

Each connection runs three cooperating tasks:

    reader  --(audio)-->  AudioBuffer  --(chunks)-->  transcriber
       \\                                                  /
        `------------> outbound queue <------------------'
                              |
                           sender

The single sender task exists because two tasks writing to one WebSocket can
interleave frames. Funnelling every outbound frame through one queue makes
ordering a property of the design instead of a race that shows up under load.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect

from gateway import backends
from gateway.config import Settings
from gateway.metrics import Metrics
from gateway.protocol import ErrorCode, ProtocolError, ServerMessage
from gateway.session import SessionState, StreamSession, drain_with_timeout

logger = logging.getLogger("gateway")

# Codes the connection cannot recover from: the session is torn down rather than
# left in a state where the client thinks it is still streaming.
FATAL_CODES = frozenset(
    {
        ErrorCode.UNSUPPORTED_AUDIO,
        ErrorCode.BACKEND_ERROR,
        ErrorCode.SESSION_TIMEOUT,
        ErrorCode.IDLE_TIMEOUT,
    }
)


def create_app(
    settings: Settings | None = None, metrics: Metrics | None = None
) -> FastAPI:
    """Build the ASGI app. Explicit factory so tests can inject configuration."""
    config = settings or Settings.from_env()
    config.validate()
    meter = metrics or Metrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Resolve the backend at startup so an unknown name fails immediately
        # rather than on the first client connection.
        app.state.backend_factory = lambda: backends.create(config.backend)
        app.state.backend_factory()
        logger.info(
            "gateway ready: backend=%s queue=%d", config.backend, config.queue_size
        )
        yield

    app = FastAPI(
        title="audio-stream-gateway",
        version="0.1.0",
        description="Real-time WebSocket gateway for streaming speech recognition.",
        lifespan=lifespan,
    )
    app.state.settings = config
    app.state.metrics = meter

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "backend": config.backend,
            "available_backends": backends.available(),
        }

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        return Response(
            content=meter.render_prometheus(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.get("/metrics.json")
    async def metrics_json() -> dict[str, Any]:
        return meter.snapshot()

    @app.websocket("/v1/stream")
    async def stream(websocket: WebSocket) -> None:
        await websocket.accept()
        session = StreamSession(app.state.backend_factory(), config, meter)
        outbound: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        started = asyncio.Event()
        disconnected = asyncio.Event()

        async def send_loop() -> None:
            while True:
                frame = await outbound.get()
                if frame is None:
                    return
                try:
                    await websocket.send_json(frame)
                except (WebSocketDisconnect, RuntimeError):
                    disconnected.set()
                    return

        async def read_loop() -> None:
            try:
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        disconnected.set()
                        session.buffer.close()
                        return
                    try:
                        if (text := message.get("text")) is not None:
                            response = session.handle_text(text)
                            if response is not None:
                                await outbound.put(response)
                                if response["type"] == ServerMessage.READY.value:
                                    started.set()
                            if session.state is SessionState.STOPPING:
                                return
                        elif (data := message.get("bytes")) is not None:
                            session.handle_audio(data)
                    except ProtocolError as exc:
                        meter.increment(f"protocol_error_{exc.code.value}")
                        await outbound.put(exc.to_frame(session.id, -1))
                        if exc.code in FATAL_CODES:
                            session.buffer.close()
                            return
            except WebSocketDisconnect:
                disconnected.set()
                session.buffer.close()

        async def transcribe_loop() -> None:
            # Nothing to transcribe until the audio contract is agreed. Waiting on
            # both events means a client that disconnects before `start` unwinds
            # cleanly instead of hanging this task forever.
            waiters = [
                asyncio.create_task(started.wait()),
                asyncio.create_task(disconnected.wait()),
            ]
            _done, pending = await asyncio.wait(
                waiters, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            if not started.is_set():
                return
            try:
                async for frame in session.transcripts():
                    await outbound.put(frame)
            except ProtocolError as exc:
                meter.increment(f"protocol_error_{exc.code.value}")
                await outbound.put(exc.to_frame(session.id, -1))

        async def guard_loop() -> None:
            """Enforce the wall-clock ceiling on a single session."""
            try:
                await asyncio.wait_for(
                    disconnected.wait(), timeout=config.max_session_seconds
                )
            except asyncio.TimeoutError:
                meter.increment("sessions_wall_clock_timeout")
                await outbound.put(
                    ProtocolError(
                        ErrorCode.SESSION_TIMEOUT,
                        f"session exceeded {config.max_session_seconds:.0f}s",
                    ).to_frame(session.id, -1)
                )
                session.buffer.close()

        sender = asyncio.create_task(send_loop())
        guard = asyncio.create_task(guard_loop())
        idle = asyncio.create_task(
            drain_with_timeout(session, config.idle_timeout_seconds)
        )

        try:
            await asyncio.gather(read_loop(), transcribe_loop())
            if not disconnected.is_set():
                await outbound.put(session.close())
        finally:
            for task in (guard, idle):
                task.cancel()
            session.close()
            await outbound.put(None)
            await sender
            if not disconnected.is_set():
                with contextlib.suppress(RuntimeError):
                    await websocket.close()

    return app


# Module-level instance for `uvicorn gateway.app:app`. Built at import time so a
# bad GATEWAY_* value fails at startup rather than on the first connection.
app = create_app()


def run() -> None:  # pragma: no cover - thin uvicorn wrapper
    """Console-script entry point."""
    import uvicorn

    settings = Settings.from_env()
    logging.basicConfig(level=settings.log_level.upper())
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
    )


if __name__ == "__main__":  # pragma: no cover
    run()
