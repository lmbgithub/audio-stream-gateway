"""Latency and throughput instrumentation.

For a streaming transcription service the headline number is not throughput, it
is time-to-first-token: how long a speaker waits before seeing anything at all.
An average hides exactly the cases users complain about, so latencies are kept
as distributions and reported at p50/p95/p99.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Distribution:
    """A bounded reservoir of observations with percentile access.

    Observations are capped at `max_samples` and the oldest are discarded, which
    keeps memory flat on a long-running process and biases the window toward
    recent behaviour — the right trade for an operational signal, and the wrong
    one for an audit trail. This is a live gauge, not a ledger.
    """

    name: str
    unit: str = "ms"
    max_samples: int = 4096
    _samples: list[float] = field(default_factory=list, repr=False)
    _count: int = 0
    _total: float = 0.0

    def observe(self, value: float) -> None:
        self._count += 1
        self._total += value
        self._samples.append(value)
        if len(self._samples) > self.max_samples:
            # Drop the oldest half at once rather than one per insert, so the
            # amortized cost stays O(1) instead of O(n) on every observation.
            del self._samples[: self.max_samples // 2]

    @property
    def count(self) -> int:
        return self._count

    @property
    def mean(self) -> float:
        return self._total / self._count if self._count else 0.0

    def percentile(self, q: float) -> float:
        """Nearest-rank percentile over the current reservoir."""
        if not self._samples:
            return 0.0
        if not 0.0 <= q <= 1.0:
            raise ValueError("percentile must be between 0 and 1")
        ordered = sorted(self._samples)
        idx = round(q * (len(ordered) - 1))
        return ordered[max(0, min(len(ordered) - 1, idx))]

    def snapshot(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "mean": round(self.mean, 3),
            "p50": round(self.percentile(0.50), 3),
            "p95": round(self.percentile(0.95), 3),
            "p99": round(self.percentile(0.99), 3),
            "unit": self.unit,
        }


class Metrics:
    """Process-wide counters and latency distributions.

    Guarded by a lock because the ASGI server may run handlers across threads,
    and a torn read on a counter is a support ticket nobody can reproduce.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._distributions: dict[str, Distribution] = {}
        self._gauges: dict[str, float] = {}
        self._started_at = time.time()

    def increment(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + amount

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def observe(self, name: str, value: float, unit: str = "ms") -> None:
        with self._lock:
            dist = self._distributions.get(name)
            if dist is None:
                dist = Distribution(name=name, unit=unit)
                self._distributions[name] = dist
            dist.observe(value)

    def counter(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def distribution(self, name: str) -> Distribution | None:
        with self._lock:
            return self._distributions.get(name)

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._distributions.clear()
            self._gauges.clear()
            self._started_at = time.time()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "uptime_seconds": round(time.time() - self._started_at, 3),
                "counters": dict(sorted(self._counters.items())),
                "gauges": dict(sorted(self._gauges.items())),
                "latency": {
                    name: dist.snapshot()
                    for name, dist in sorted(self._distributions.items())
                },
            }

    def render_prometheus(self, prefix: str = "gateway") -> str:
        """Render the current state in Prometheus text exposition format."""
        snap = self.snapshot()
        lines: list[str] = []

        lines.append(f"# HELP {prefix}_uptime_seconds Seconds since process start.")
        lines.append(f"# TYPE {prefix}_uptime_seconds gauge")
        lines.append(f"{prefix}_uptime_seconds {snap['uptime_seconds']}")

        for name, value in snap["counters"].items():
            metric = f"{prefix}_{name}_total"
            lines.append(f"# TYPE {metric} counter")
            lines.append(f"{metric} {value}")

        for name, value in snap["gauges"].items():
            metric = f"{prefix}_{name}"
            lines.append(f"# TYPE {metric} gauge")
            lines.append(f"{metric} {value}")

        for name, stats in snap["latency"].items():
            metric = f"{prefix}_{name}_{stats['unit']}"
            lines.append(f"# TYPE {metric} summary")
            for quantile in ("p50", "p95", "p99"):
                q = "0." + quantile[1:]
                lines.append(f'{metric}{{quantile="{q}"}} {stats[quantile]}')
            lines.append(f"{metric}_count {stats['count']}")

        return "\n".join(lines) + "\n"


class SessionTimer:
    """Per-session latency bookkeeping.

    Time-to-first-byte is measured from the first audio chunk received, not from
    socket open: the client controls when it starts speaking, and charging the
    gateway for the client's silence makes the number useless for comparing
    backends.
    """

    def __init__(self, clock=time.perf_counter) -> None:
        self._clock = clock
        self.opened_at = clock()
        self.first_audio_at: float | None = None
        self.first_transcript_at: float | None = None
        self.closed_at: float | None = None
        self.audio_bytes = 0
        self.chunks_received = 0
        self.chunks_dropped = 0
        self.partials_sent = 0
        self.finals_sent = 0

    def record_audio(self, size: int) -> None:
        if self.first_audio_at is None:
            self.first_audio_at = self._clock()
        self.chunks_received += 1
        self.audio_bytes += size

    def record_drop(self) -> None:
        self.chunks_dropped += 1

    def record_transcript(self, *, is_final: bool) -> None:
        if self.first_transcript_at is None:
            self.first_transcript_at = self._clock()
        if is_final:
            self.finals_sent += 1
        else:
            self.partials_sent += 1

    def close(self) -> None:
        if self.closed_at is None:
            self.closed_at = self._clock()

    @property
    def ttfb_ms(self) -> float | None:
        """Milliseconds from first audio in to first transcript out."""
        if self.first_audio_at is None or self.first_transcript_at is None:
            return None
        return (self.first_transcript_at - self.first_audio_at) * 1000.0

    @property
    def duration_ms(self) -> float:
        end = self.closed_at if self.closed_at is not None else self._clock()
        return (end - self.opened_at) * 1000.0

    def summary(self) -> dict[str, Any]:
        ttfb = self.ttfb_ms
        return {
            "duration_ms": round(self.duration_ms, 3),
            "ttfb_ms": None if ttfb is None else round(ttfb, 3),
            "audio_bytes": self.audio_bytes,
            "chunks_received": self.chunks_received,
            "chunks_dropped": self.chunks_dropped,
            "partials_sent": self.partials_sent,
            "finals_sent": self.finals_sent,
        }


def percentiles(
    values: Iterable[float], quantiles: tuple[float, ...] = (0.5, 0.95, 0.99)
) -> dict[str, float]:
    """Standalone percentile helper for ad-hoc analysis of collected latencies."""
    ordered = sorted(values)
    if not ordered:
        return {f"p{int(q * 100)}": 0.0 for q in quantiles}
    out = {}
    for q in quantiles:
        idx = round(q * (len(ordered) - 1))
        out[f"p{int(q * 100)}"] = ordered[max(0, min(len(ordered) - 1, idx))]
    return out
