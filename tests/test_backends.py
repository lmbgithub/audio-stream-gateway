import pytest

from gateway import backends
from gateway.backends.base import ASRBackend, AudioFormat, BackendError
from gateway.backends.mock import EchoBackend, MockBackend


async def feed(chunks):
    for c in chunks:
        yield c


PCM = AudioFormat(sample_rate=16000, encoding="linear16")


def test_registry_lists_builtins():
    assert "mock" in backends.available()
    assert "echo" in backends.available()


def test_create_returns_an_instance():
    assert backends.create("mock").name == "mock"
    assert backends.create("MOCK").name == "mock"


def test_unknown_backend_names_the_alternatives():
    with pytest.raises(BackendError, match="available"):
        backends.create("whisper-turbo-9000")


def test_custom_backend_can_register():
    backends.register("temp-test", EchoBackend)
    try:
        assert backends.create("temp-test").name == "echo"
    finally:
        backends._REGISTRY.pop("temp-test", None)


def test_backends_satisfy_the_protocol():
    assert isinstance(MockBackend(), ASRBackend)
    assert isinstance(EchoBackend(), ASRBackend)


def test_audio_format_byte_rate():
    assert PCM.bytes_per_second == 32000
    assert AudioFormat(8000, "mulaw").bytes_per_second == 8000


def test_supports_rejects_unknown_encoding():
    assert MockBackend().supports(PCM)
    assert not MockBackend().supports(AudioFormat(16000, "opus"))


async def test_mock_streams_partials_then_finals():
    results = [t async for t in MockBackend().stream(PCM, feed([b"x" * 320] * 4))]
    assert results, "backend produced nothing"
    assert results[0].text == "the"
    assert results[0].is_final is False
    assert results[3].text == "the quick brown fox"
    assert results[3].is_final is True


async def test_mock_resets_text_after_a_final():
    results = [t async for t in MockBackend().stream(PCM, feed([b"x" * 320] * 6))]
    after_final = results[4]
    assert after_final.text == "jumps"
    assert after_final.is_final is False


async def test_mock_flushes_an_unterminated_tail():
    # 5 chunks ends mid-phrase; the tail must still be emitted as a final.
    results = [t async for t in MockBackend().stream(PCM, feed([b"x" * 320] * 5))]
    assert results[-1].is_final is True
    assert results[-1].text == "jumps"


async def test_mock_with_no_audio_produces_nothing():
    assert [t async for t in MockBackend().stream(PCM, feed([]))] == []


async def test_mock_rejects_unsupported_format():
    with pytest.raises(BackendError, match="unsupported audio format"):
        [t async for t in MockBackend().stream(AudioFormat(16000, "opus"), feed([b"x"]))]


async def test_mock_can_simulate_failure():
    backend = MockBackend(fail_after=2)
    with pytest.raises(BackendError, match="simulated"):
        [t async for t in backend.stream(PCM, feed([b"x" * 320] * 5))]


async def test_mock_timestamps_are_monotonic():
    results = [t async for t in MockBackend().stream(PCM, feed([b"x" * 3200] * 6))]
    ends = [t.end_ms for t in results]
    assert ends == sorted(ends)


async def test_echo_backend_reports_running_totals():
    results = [t async for t in EchoBackend().stream(PCM, feed([b"ab", b"cde"]))]
    assert results[0].text == "received 2 bytes"
    assert results[1].text == "received 5 bytes"
    assert results[-1].is_final is True
