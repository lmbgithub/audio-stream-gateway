import pytest

from gateway.metrics import Distribution, Metrics, SessionTimer, percentiles


def test_distribution_percentiles():
    d = Distribution("lat")
    for v in range(1, 101):
        d.observe(float(v))
    assert d.count == 100
    assert d.percentile(0.5) == pytest.approx(50, abs=1)
    assert d.percentile(0.99) == pytest.approx(99, abs=1)
    assert d.mean == pytest.approx(50.5)


def test_distribution_is_empty_safe():
    d = Distribution("lat")
    assert d.percentile(0.5) == 0.0
    assert d.mean == 0.0
    assert d.snapshot()["count"] == 0


def test_distribution_rejects_bad_quantiles():
    d = Distribution("lat")
    d.observe(1.0)
    with pytest.raises(ValueError):
        d.percentile(1.5)


def test_distribution_reservoir_is_bounded_but_counts_everything():
    d = Distribution("lat", max_samples=100)
    for v in range(1000):
        d.observe(float(v))
    assert d.count == 1000            # every observation counted
    assert len(d._samples) <= 100     # memory stays flat


def test_counters_and_gauges():
    m = Metrics()
    m.increment("sessions_started")
    m.increment("sessions_started", 4)
    m.gauge("active_sessions", 3)
    assert m.counter("sessions_started") == 5
    assert m.snapshot()["gauges"]["active_sessions"] == 3


def test_unknown_counter_reads_zero():
    assert Metrics().counter("never_touched") == 0


def test_observe_creates_the_distribution_lazily():
    m = Metrics()
    m.observe("ttfb", 12.5)
    assert m.distribution("ttfb").count == 1
    assert m.distribution("absent") is None


def test_reset_clears_everything():
    m = Metrics()
    m.increment("a")
    m.observe("b", 1.0)
    m.reset()
    assert m.counter("a") == 0
    assert m.distribution("b") is None


def test_prometheus_rendering_is_well_formed():
    m = Metrics()
    m.increment("sessions_started", 2)
    m.gauge("active_sessions", 1)
    m.observe("time_to_first_transcript", 42.0)
    text = m.render_prometheus()
    assert "gateway_sessions_started_total 2" in text
    assert "gateway_active_sessions 1" in text
    assert 'gateway_time_to_first_transcript_ms{quantile="0.95"}' in text
    assert text.endswith("\n")
    # every non-comment line must be "name value"
    for line in text.strip().splitlines():
        if not line.startswith("#"):
            assert len(line.rsplit(" ", 1)) == 2


def test_session_timer_ttfb_measures_from_first_audio():
    ticks = iter([0.0, 1.0, 1.5, 3.0])
    t = SessionTimer(clock=lambda: next(ticks))  # opened_at = 0.0
    t.record_audio(320)          # first_audio_at = 1.0
    t.record_transcript(is_final=False)  # first_transcript_at = 1.5
    assert t.ttfb_ms == pytest.approx(500.0)


def test_session_timer_ttfb_is_none_without_a_transcript():
    t = SessionTimer()
    t.record_audio(10)
    assert t.ttfb_ms is None


def test_session_timer_counts_and_summary():
    t = SessionTimer()
    t.record_audio(100)
    t.record_audio(200)
    t.record_drop()
    t.record_transcript(is_final=False)
    t.record_transcript(is_final=True)
    t.close()
    s = t.summary()
    assert s["audio_bytes"] == 300
    assert s["chunks_received"] == 2
    assert s["chunks_dropped"] == 1
    assert s["partials_sent"] == 1
    assert s["finals_sent"] == 1
    assert s["duration_ms"] >= 0


def test_close_is_idempotent():
    t = SessionTimer()
    t.close()
    first = t.closed_at
    t.close()
    assert t.closed_at == first


def test_percentiles_helper():
    assert percentiles([])["p50"] == 0.0
    assert percentiles([1, 2, 3, 4, 5])["p50"] == 3
