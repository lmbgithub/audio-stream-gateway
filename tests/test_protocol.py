import json

import pytest

from gateway.protocol import (
    ClientMessage,
    ErrorCode,
    ProtocolError,
    ServerMessage,
    StartRequest,
    decode_client_frame,
    transcript_frame,
)


def test_decodes_a_known_message():
    message, payload = decode_client_frame(
        json.dumps({"type": "start", "sample_rate": 8000})
    )
    assert message is ClientMessage.START
    assert payload["sample_rate"] == 8000


def test_message_type_is_case_insensitive():
    message, _ = decode_client_frame('{"type": "START"}')
    assert message is ClientMessage.START


def test_invalid_json_raises_with_a_code():
    with pytest.raises(ProtocolError) as exc:
        decode_client_frame("{not json")
    assert exc.value.code is ErrorCode.INVALID_JSON


def test_non_object_payload_is_rejected():
    with pytest.raises(ProtocolError) as exc:
        decode_client_frame('["start"]')
    assert exc.value.code is ErrorCode.INVALID_JSON


def test_missing_type_is_rejected():
    with pytest.raises(ProtocolError) as exc:
        decode_client_frame('{"sample_rate": 16000}')
    assert exc.value.code is ErrorCode.UNKNOWN_TYPE


def test_unknown_type_lists_the_allowed_values():
    with pytest.raises(ProtocolError) as exc:
        decode_client_frame('{"type": "transcribe"}')
    assert exc.value.code is ErrorCode.UNKNOWN_TYPE
    assert "start" in exc.value.message


def test_start_request_defaults():
    req = StartRequest.parse({"type": "start"})
    assert req.sample_rate == 16000
    assert req.encoding == "linear16"
    assert req.channels == 1
    assert req.interim_results is True


def test_start_request_keeps_unknown_keys_as_metadata():
    req = StartRequest.parse({"type": "start", "tenant": "acme", "trace_id": "abc"})
    assert req.metadata["tenant"] == "acme"
    assert req.metadata["trace_id"] == "abc"


def test_start_request_rejects_non_numeric_sample_rate():
    with pytest.raises(ProtocolError) as exc:
        StartRequest.parse({"type": "start", "sample_rate": "fast"})
    assert exc.value.code is ErrorCode.UNSUPPORTED_AUDIO


def test_transcript_frame_shape():
    frame = transcript_frame(
        session_id="s1",
        seq=3,
        text="hello",
        is_final=True,
        start_ms=0,
        end_ms=500,
        confidence=0.9123456,
    )
    assert frame["type"] == ServerMessage.FINAL.value
    assert frame["is_final"] is True
    assert frame["confidence"] == 0.9123
    assert frame["session_id"] == "s1"
    assert frame["seq"] == 3


def test_partial_frame_omits_absent_fields():
    frame = transcript_frame(session_id="s1", seq=1, text="he", is_final=False)
    assert frame["type"] == ServerMessage.PARTIAL.value
    assert "confidence" not in frame
    assert "start_ms" not in frame


def test_error_serializes_to_a_frame():
    frame = ProtocolError(ErrorCode.CHUNK_TOO_LARGE, "too big").to_frame("s1", 7)
    assert frame["type"] == ServerMessage.ERROR.value
    assert frame["code"] == "chunk_too_large"
    assert frame["message"] == "too big"
