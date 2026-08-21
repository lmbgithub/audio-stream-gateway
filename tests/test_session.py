import json

import pytest

from gateway.backends.mock import MockBackend
from gateway.config import Settings
from gateway.metrics import Metrics
from gateway.protocol import ErrorCode, ProtocolError, ServerMessage
from gateway.session import AudioBuffer, SessionState, StreamSession

CHUNK = b"\x00" * 320


def make_session(**overrides):
    settings = Settings(**{"queue_size": 4, **overrides})
    return StreamSession(MockBackend(), settings, Metrics())


def start_frame(**kwargs):
    return json.dumps({"type": "start", **kwargs})


# -- AudioBuffer ---------------------------------------------------------


def test_buffer_rejects_zero_capacity():
    with pytest.raises(ValueError):
        AudioBuffer(0)


def test_buffer_accepts_until_full():
    buf = AudioBuffer(3)
    assert all(buf.push(bytes([i])) for i in range(3))
    assert buf.dropped == 0
    assert len(buf) == 3


def test_buffer_drops_the_oldest_chunk_when_full():
    buf = AudioBuffer(2)
    buf.push(b"a")
    buf.push(b"b")
    assert buf.push(b"c") is False   # signalled, not silent
    assert buf.dropped == 1
    assert list(buf._items) == [b"b", b"c"]  # newest retained, oldest gone


def test_buffer_cannot_be_pushed_after_close():
    buf = AudioBuffer(2)
    buf.close()
    with pytest.raises(RuntimeError):
        buf.push(b"a")


async def test_buffer_iteration_drains_then_stops():
    buf = AudioBuffer(4)
    buf.push(b"a")
    buf.push(b"b")
    buf.close()
    assert [c async for c in buf] == [b"a", b"b"]


async def test_buffer_iteration_ends_on_close_with_no_items():
    buf = AudioBuffer(4)
    buf.close()
    assert [c async for c in buf] == []


# -- state machine -------------------------------------------------------


def test_start_returns_ready_and_moves_to_running():
    s = make_session()
    frame = s.handle_text(start_frame())
    assert frame["type"] == ServerMessage.READY.value
    assert frame["backend"] == "mock"
    assert s.state is SessionState.RUNNING


def test_start_twice_is_rejected():
    s = make_session()
    s.handle_text(start_frame())
    with pytest.raises(ProtocolError) as exc:
        s.handle_text(start_frame())
    assert exc.value.code is ErrorCode.INVALID_STATE


def test_audio_before_start_is_rejected():
    s = make_session()
    with pytest.raises(ProtocolError) as exc:
        s.handle_audio(CHUNK)
    assert exc.value.code is ErrorCode.INVALID_STATE


def test_stop_before_start_is_rejected():
    with pytest.raises(ProtocolError) as exc:
        make_session().handle_text('{"type": "stop"}')
    assert exc.value.code is ErrorCode.INVALID_STATE


def test_stop_moves_to_stopping_and_closes_the_buffer():
    s = make_session()
    s.handle_text(start_frame())
    assert s.handle_text('{"type": "stop"}') is None
    assert s.state is SessionState.STOPPING
    assert s.buffer.closed


def test_stop_is_idempotent():
    s = make_session()
    s.handle_text(start_frame())
    s.handle_text('{"type": "stop"}')
    assert s.handle_text('{"type": "stop"}') is None


def test_ping_is_answered_in_any_state():
    s = make_session()
    assert s.handle_text('{"type": "ping"}')["type"] == ServerMessage.PONG.value


def test_channel_count_above_the_deployment_limit_is_rejected():
    s = make_session(max_channels=1)
    with pytest.raises(ProtocolError) as exc:
        s.handle_text(start_frame(channels=2))
    assert exc.value.code is ErrorCode.UNSUPPORTED_AUDIO
    assert "at most" in exc.value.message


def test_encoding_the_backend_cannot_handle_is_rejected():
    s = make_session()
    with pytest.raises(ProtocolError) as exc:
        s.handle_text(start_frame(encoding="opus"))
    assert exc.value.code is ErrorCode.UNSUPPORTED_AUDIO


def test_oversized_chunk_is_rejected_without_entering_the_buffer():
    s = make_session(max_chunk_bytes=16)
    s.handle_text(start_frame())
    with pytest.raises(ProtocolError) as exc:
        s.handle_audio(b"x" * 17)
    assert exc.value.code is ErrorCode.CHUNK_TOO_LARGE
    assert len(s.buffer) == 0


def test_sequence_numbers_increase_monotonically():
    s = make_session()
    seqs = [s.handle_text('{"type": "ping"}')["seq"] for _ in range(5)]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == 5


# -- transcription and accounting ---------------------------------------


async def test_transcripts_require_a_started_session():
    s = make_session()
    with pytest.raises(ProtocolError):
        [f async for f in s.transcripts()]


async def test_end_to_end_produces_frames_and_records_ttfb():
    s = make_session(queue_size=64)
    s.handle_text(start_frame())
    for _ in range(4):
        s.handle_audio(CHUNK)
    s.handle_text('{"type": "stop"}')

    frames = [f async for f in s.transcripts()]
    assert [f["type"] for f in frames][:3] == ["partial", "partial", "partial"]
    assert frames[-1]["is_final"] is True
    assert s.metrics.distribution("time_to_first_transcript").count == 1
    assert s.metrics.counter("transcripts_partial") == 3
    assert s.metrics.counter("transcripts_final") == 1


async def test_backend_failure_becomes_a_protocol_error():
    settings = Settings(queue_size=64)
    s = StreamSession(MockBackend(fail_after=1), settings, Metrics())
    s.handle_text(start_frame())
    for _ in range(4):
        s.handle_audio(CHUNK)
    s.handle_text('{"type": "stop"}')
    with pytest.raises(ProtocolError) as exc:
        [f async for f in s.transcripts()]
    assert exc.value.code is ErrorCode.BACKEND_ERROR
    assert s.metrics.counter("backend_errors") == 1


def test_overflow_is_counted_on_the_session_and_the_meter():
    s = make_session(queue_size=2)
    s.handle_text(start_frame())
    for _ in range(5):
        s.handle_audio(CHUNK)
    assert s.timer.chunks_dropped == 3
    assert s.metrics.counter("audio_chunks_dropped") == 3
    assert s.metrics.counter("audio_chunks_received") == 5


def test_close_reports_a_summary_and_is_idempotent():
    s = make_session()
    s.handle_text(start_frame())
    s.handle_audio(CHUNK)
    frame = s.close()
    assert frame["type"] == ServerMessage.CLOSED.value
    assert frame["audio_bytes"] == 320
    assert frame["buffer_capacity"] == 4
    assert s.state is SessionState.CLOSED
    assert s.close()["type"] == ServerMessage.CLOSED.value
    assert s.metrics.counter("sessions_closed") == 1


def test_metrics_frame_is_available_mid_session():
    s = make_session()
    s.handle_text(start_frame())
    s.handle_audio(CHUNK)
    assert s.metrics_frame()["type"] == ServerMessage.METRICS.value
