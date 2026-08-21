import pytest

from gateway.config import ConfigError, Settings


def test_defaults_work_with_no_environment():
    s = Settings.from_env({})
    assert s.backend == "mock"
    assert s.queue_size == 64
    assert s.port == 8000


def test_environment_overrides_are_applied():
    s = Settings.from_env({"GATEWAY_PORT": "9000", "GATEWAY_BACKEND": "ECHO", "GATEWAY_QUEUE_SIZE": "8"})
    assert s.port == 9000
    assert s.backend == "echo"  # normalized to lowercase
    assert s.queue_size == 8


def test_blank_values_fall_back_to_defaults():
    assert Settings.from_env({"GATEWAY_PORT": ""}).port == 8000


def test_non_integer_is_rejected_with_a_useful_message():
    with pytest.raises(ConfigError, match="GATEWAY_PORT"):
        Settings.from_env({"GATEWAY_PORT": "eight thousand"})


def test_non_numeric_float_is_rejected():
    with pytest.raises(ConfigError, match="IDLE_TIMEOUT"):
        Settings.from_env({"GATEWAY_IDLE_TIMEOUT_SECONDS": "soon"})


@pytest.mark.parametrize(
    "env",
    [
        {"GATEWAY_QUEUE_SIZE": "0"},
        {"GATEWAY_SAMPLE_RATE": "0"},
        {"GATEWAY_MAX_CHANNELS": "0"},
        {"GATEWAY_MAX_CHUNK_BYTES": "0"},
        {"GATEWAY_PORT": "70000"},
        {"GATEWAY_MAX_SESSION_SECONDS": "-1"},
    ],
)
def test_invalid_ranges_fail_fast(env):
    with pytest.raises(ConfigError):
        Settings.from_env(env)


def test_settings_serialize():
    assert Settings().to_dict()["backend"] == "mock"
