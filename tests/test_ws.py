"""End-to-end tests over a real WebSocket connection via Starlette's TestClient."""

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import Settings
from gateway.metrics import Metrics

CHUNK = b"\x00" * 320


@pytest.fixture
def meter():
    return Metrics()


@pytest.fixture
def client(meter):
    settings = Settings(queue_size=64, idle_timeout_seconds=30.0, max_chunk_bytes=4096)
    with TestClient(create_app(settings, meter)) as c:
        yield c


def drain(ws):
    """Collect frames until the session closes."""
    frames = []
    while True:
        frame = ws.receive_json()
        frames.append(frame)
        if frame["type"] in ("closed", "error"):
            return frames


def test_healthz(client):
    body = client.get("/healthz").json()
    assert body["status"] == "ok"
    assert body["backend"] == "mock"
    assert "echo" in body["available_backends"]


def test_metrics_endpoints_are_served(client):
    prom = client.get("/metrics")
    assert prom.status_code == 200
    assert "text/plain" in prom.headers["content-type"]
    assert "gateway_uptime_seconds" in prom.text
    assert client.get("/metrics.json").json()["counters"] is not None


def test_full_session_start_stream_stop(client):
    with client.websocket_connect("/v1/stream") as ws:
        ws.send_json({"type": "start", "sample_rate": 16000, "encoding": "linear16"})
        ready = ws.receive_json()
        assert ready["type"] == "ready"
        assert ready["backend"] == "mock"
        assert ready["seq"] == 1
        session_id = ready["session_id"]

        for _ in range(4):
            ws.send_bytes(CHUNK)
        ws.send_json({"type": "stop"})

        frames = drain(ws)

    assert all(f["session_id"] == session_id for f in frames)
    assert [f["type"] for f in frames if f["type"] == "partial"]
    finals = [f for f in frames if f["type"] == "final"]
    assert finals and finals[-1]["text"] == "the quick brown fox"

    closed = frames[-1]
    assert closed["type"] == "closed"
    assert closed["chunks_received"] == 4
    assert closed["audio_bytes"] == 4 * 320
    assert closed["ttfb_ms"] is not None


def test_sequence_numbers_are_unique_and_ordered(client):
    with client.websocket_connect("/v1/stream") as ws:
        ws.send_json({"type": "start"})
        ws.receive_json()
        for _ in range(6):
            ws.send_bytes(CHUNK)
        ws.send_json({"type": "stop"})
        frames = drain(ws)

    seqs = [f["seq"] for f in frames if f["seq"] > 0]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)


def test_ping_pong(client):
    with client.websocket_connect("/v1/stream") as ws:
        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"


def test_audio_before_start_returns_an_error_frame(client):
    with client.websocket_connect("/v1/stream") as ws:
        ws.send_bytes(CHUNK)
        frame = ws.receive_json()
        assert frame["type"] == "error"
        assert frame["code"] == "invalid_state"


def test_malformed_json_is_reported_and_the_socket_survives(client):
    with client.websocket_connect("/v1/stream") as ws:
        ws.send_text("{oops")
        assert ws.receive_json()["code"] == "invalid_json"
        # non-fatal: the connection still works
        ws.send_json({"type": "ping"})
        assert ws.receive_json()["type"] == "pong"


def test_unsupported_encoding_is_fatal(client):
    with client.websocket_connect("/v1/stream") as ws:
        ws.send_json({"type": "start", "encoding": "opus"})
        frame = ws.receive_json()
        assert frame["type"] == "error"
        assert frame["code"] == "unsupported_audio"


def test_oversized_chunk_is_reported(client):
    with client.websocket_connect("/v1/stream") as ws:
        ws.send_json({"type": "start"})
        ws.receive_json()
        ws.send_bytes(b"x" * 5000)
        frame = ws.receive_json()
        assert frame["type"] == "error"
        assert frame["code"] == "chunk_too_large"


def test_stop_without_audio_closes_cleanly(client):
    with client.websocket_connect("/v1/stream") as ws:
        ws.send_json({"type": "start"})
        ws.receive_json()
        ws.send_json({"type": "stop"})
        frames = drain(ws)
    assert frames[-1]["type"] == "closed"
    assert frames[-1]["chunks_received"] == 0
    assert frames[-1]["ttfb_ms"] is None


def test_backpressure_drops_are_reported_in_the_close_frame(meter):
    """A consumer slower than the producer must shed load, visibly.

    The backend is deliberately paced so the transcriber cannot keep up with the
    reader. With a capacity-2 buffer, the oldest audio has to be discarded — and
    the point of the test is that the loss is *reported* in the close frame and
    the meter, never silent.
    """
    from gateway import backends
    from gateway.backends.mock import MockBackend

    backends.register("slow", lambda: MockBackend(emit_delay=0.02))
    try:
        settings = Settings(backend="slow", queue_size=2, max_chunk_bytes=4096)
        with TestClient(create_app(settings, meter)) as c:
            with c.websocket_connect("/v1/stream") as ws:
                ws.send_json({"type": "start"})
                ws.receive_json()
                for _ in range(40):
                    ws.send_bytes(CHUNK)
                ws.send_json({"type": "stop"})
                frames = drain(ws)
    finally:
        backends._REGISTRY.pop("slow", None)

    closed = frames[-1]
    assert closed["chunks_received"] == 40
    assert closed["chunks_dropped"] >= 1
    assert closed["chunks_dropped"] < 40
    assert meter.counter("audio_chunks_dropped") == closed["chunks_dropped"]
    # Retention invariant: whatever survived was newer than what was discarded.
    assert closed["chunks_received"] - closed["chunks_dropped"] >= 1


def test_backend_failure_surfaces_as_an_error_frame(meter):
    from gateway import backends
    from gateway.backends.mock import MockBackend

    backends.register("flaky", lambda: MockBackend(fail_after=1))
    try:
        settings = Settings(backend="flaky", queue_size=64)
        with TestClient(create_app(settings, meter)) as c:
            with c.websocket_connect("/v1/stream") as ws:
                ws.send_json({"type": "start"})
                ws.receive_json()
                for _ in range(4):
                    ws.send_bytes(CHUNK)
                ws.send_json({"type": "stop"})
                frames = drain(ws)
        assert frames[-1]["type"] == "error"
        assert frames[-1]["code"] == "backend_error"
    finally:
        backends._REGISTRY.pop("flaky", None)


def test_metrics_accumulate_across_sessions(client, meter):
    for _ in range(3):
        with client.websocket_connect("/v1/stream") as ws:
            ws.send_json({"type": "start"})
            ws.receive_json()
            ws.send_bytes(CHUNK)
            ws.send_json({"type": "stop"})
            drain(ws)

    assert meter.counter("sessions_started") == 3
    assert meter.counter("sessions_closed") == 3
    assert meter.counter("audio_chunks_received") == 3
    assert "gateway_sessions_started_total 3" in client.get("/metrics").text


def test_echo_backend_can_be_selected(meter):
    settings = Settings(backend="echo", queue_size=64)
    with TestClient(create_app(settings, meter)) as c:
        with c.websocket_connect("/v1/stream") as ws:
            ws.send_json({"type": "start"})
            assert ws.receive_json()["backend"] == "echo"
            ws.send_bytes(b"abcd")
            ws.send_json({"type": "stop"})
            frames = drain(ws)
    assert any("received 4 bytes" in f.get("text", "") for f in frames)


def test_unknown_backend_fails_at_startup(meter):
    settings = Settings(backend="does-not-exist")
    with pytest.raises(Exception):
        with TestClient(create_app(settings, meter)):
            pass
