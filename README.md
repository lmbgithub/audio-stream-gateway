# audio-stream-gateway

Real-time WebSocket gateway for streaming speech recognition, with latency
instrumentation and explicit backpressure.

Accepts a live audio stream over a WebSocket, forwards it to a pluggable ASR
backend, streams partial and final transcripts back, and measures what actually
matters in a voice pipeline: how long the speaker waits before seeing the first
word.

Ships with a deterministic mock backend, so it runs end to end the moment you
clone it. No API keys, no model downloads, no cloud account.

```
$ python client/demo_client.py --seconds 2
connecting to ws://localhost:8000/v1/stream
session 204a601919bc4141 ready on backend mock

  >>> the quick brown fox
  >>> jumps over the lazy dog
  >>> and keeps on running

server-side summary:
  duration_ms        2216.403
  ttfb_ms            0.031
  chunks_received    100
  chunks_dropped     0

client-observed TTFB: 0.4 ms
```

## Why this exists

Wiring an ASR vendor into a product is easy. Running one in production is where
it gets interesting, and three problems show up every time:

**Latency is the product.** For streaming transcription, throughput is close to
irrelevant and time-to-first-token is close to everything. This gateway measures
TTFB from the first audio chunk received — not from socket open, because the
client controls when it starts speaking and charging the gateway for the
client's silence makes the number useless for comparing backends. Latencies are
kept as distributions and reported at p50/p95/p99, because an average hides
exactly the sessions users complain about.

**A speaking human cannot be backpressured.** The usual bounded-queue reflex is
to block the producer or drop the newest arrival. Both are wrong here. Blocking
propagates backpressure to a client that physically cannot slow down; dropping
the newest keeps the _stalest_ audio in the buffer and maximises the latency the
user perceives. This gateway drops the **oldest** chunk, keeping the transcript
close to the present — and counts every drop, because silent data loss becomes
an unreproducible accuracy complaint three weeks later.

**Vendors get swapped.** Transport, session lifecycle, backpressure and metrics
belong to the gateway. Turning audio into text belongs to the backend. Keeping
that line sharp is what makes a vendor change a one-file change, and it is what
lets the entire system be tested without credentials or network access.

## Architecture

Each connection runs three cooperating tasks:

```
 reader  --(audio chunks)-->  AudioBuffer  --(async iterator)-->  transcriber
    \                          (bounded,                              /
     \                       drop-oldest)                            /
      `------------------> outbound queue <------------------------'
                                 |
                              sender  --> WebSocket
```

The single sender task is deliberate. Two tasks writing to one WebSocket can
interleave frames; funnelling every outbound frame through one queue makes
ordering a property of the design rather than a race that only appears under
load.

| Module        | Responsibility                                                                |
| ------------- | ----------------------------------------------------------------------------- |
| `protocol.py` | Frame schemas, message types, closed set of error codes                       |
| `session.py`  | State machine, `AudioBuffer`, per-session accounting — no transport knowledge |
| `metrics.py`  | Counters, gauges, latency distributions, Prometheus rendering                 |
| `backends/`   | The `ASRBackend` protocol and the built-in mock/echo backends                 |
| `app.py`      | ASGI app, WebSocket endpoint, task orchestration                              |

`session.py` knows nothing about WebSockets, which is why the interesting
behaviour — the state machine, the drop policy, the timeout paths — is testable
without a socket or a running server.

## Quick start

```bash
git clone https://github.com/<your-username>/audio-stream-gateway.git
cd audio-stream-gateway
pip install -e ".[dev]" websockets

uvicorn gateway.app:app --reload          # terminal 1
python client/demo_client.py --seconds 3  # terminal 2
```

Docker:

```bash
docker build -t audio-stream-gateway .
docker run -p 8000:8000 audio-stream-gateway
```

## Protocol

Control frames are JSON text; audio frames are binary. Audio stays off the JSON
path deliberately — a 20 ms PCM chunk is 640 bytes and base64 would add a third
to that on every frame for no benefit.

**Client → server**

```jsonc
{"type": "start", "sample_rate": 16000, "encoding": "linear16", "channels": 1}
<binary audio frames>
{"type": "stop"}
{"type": "ping"}
```

**Server → client**

```jsonc
{"type": "ready",   "session_id": "...", "seq": 1, "backend": "mock", ...}
{"type": "partial", "session_id": "...", "seq": 2, "text": "the quick", "is_final": false}
{"type": "final",   "session_id": "...", "seq": 5, "text": "the quick brown fox", "is_final": true, "confidence": 0.95}
{"type": "closed",  "session_id": "...", "seq": 9, "duration_ms": 2216.4, "ttfb_ms": 31.2, "chunks_received": 100, "chunks_dropped": 0}
{"type": "error",   "code": "chunk_too_large", "message": "..."}
```

Every server frame carries the session id and a monotonically increasing `seq`,
so a client can detect gaps without relying on delivery order.

Error codes are a closed set, so clients branch on `code` and never on prose:
`invalid_json`, `unknown_type`, `invalid_state`, `unsupported_audio`,
`chunk_too_large`, `session_timeout`, `idle_timeout`, `backend_error`.
Recoverable errors (a malformed frame) leave the connection usable; fatal ones
(unsupported audio, backend failure) tear the session down rather than leaving
the client believing it is still streaming.

## Adding a backend

Implement two methods. No base class to inherit, no registration decorator
required:

```python
from gateway import backends
from gateway.backends.base import AudioFormat, Transcript

class MyVendorBackend:
    name = "my-vendor"

    def supports(self, audio: AudioFormat) -> bool:
        return audio.encoding in {"linear16", "flac"}

    async def stream(self, audio, chunks):
        async with vendor_client.connect(rate=audio.sample_rate) as upstream:
            async for chunk in chunks:
                await upstream.send(chunk)
                async for event in upstream.drain():
                    yield Transcript(text=event.text, is_final=event.is_final)

backends.register("my-vendor", MyVendorBackend)
```

Then `GATEWAY_BACKEND=my-vendor`. The contract requires that `stream` does not
buffer the whole session — the entire point is that the first partial can be
emitted before the last chunk arrives.

## Configuration

Every value has a working default; the gateway runs with an empty environment.
See [`.env.example`](.env.example).

| Variable                       | Default    | Purpose                                                 |
| ------------------------------ | ---------- | ------------------------------------------------------- |
| `GATEWAY_BACKEND`              | `mock`     | Which registered backend to load                        |
| `GATEWAY_QUEUE_SIZE`           | `64`       | Inbound buffer depth before the oldest chunk is dropped |
| `GATEWAY_SAMPLE_RATE`          | `16000`    | Expected sample rate                                    |
| `GATEWAY_ENCODING`             | `linear16` | Expected encoding                                       |
| `GATEWAY_MAX_CHANNELS`         | `1`        | Channel ceiling for this deployment                     |
| `GATEWAY_MAX_CHUNK_BYTES`      | `65536`    | Per-frame size limit                                    |
| `GATEWAY_MAX_SESSION_SECONDS`  | `300`      | Wall-clock ceiling on one session                       |
| `GATEWAY_IDLE_TIMEOUT_SECONDS` | `30`       | Close a session that stops sending audio                |

Bad values fail at startup, not mid-session: an unparseable `GATEWAY_PORT`
raises before the first connection is accepted, and an unknown backend name
fails during app startup rather than on a client's first request.

## Observability

| Endpoint            | Purpose                                       |
| ------------------- | --------------------------------------------- |
| `GET /healthz`      | Liveness, active backend, registered backends |
| `GET /metrics`      | Prometheus text exposition                    |
| `GET /metrics.json` | Same data as JSON                             |

```
gateway_sessions_started_total 3
gateway_audio_chunks_received_total 100
gateway_audio_chunks_dropped_total 0
gateway_time_to_first_transcript_ms{quantile="0.95"} 31.2
gateway_session_duration_ms{quantile="0.50"} 2216.4
```

Latency reservoirs are bounded and discard the oldest observations, keeping
memory flat on a long-running process and biasing the window toward recent
behaviour. That is the right trade for an operational gauge and the wrong one
for an audit trail — this is a live signal, not a ledger.

## Tests

```bash
pytest -q     # 91 tests
```

| Suite              | Covers                                                                   |
| ------------------ | ------------------------------------------------------------------------ |
| `test_config.py`   | Env parsing, range validation, fail-fast behaviour                       |
| `test_protocol.py` | Frame decode/encode, every error code path                               |
| `test_metrics.py`  | Percentiles, bounded reservoir, TTFB arithmetic on an injected clock     |
| `test_backends.py` | Registry, streaming contract, tail flushing, failure simulation          |
| `test_session.py`  | State machine, drop-oldest retention, accounting, idempotent close       |
| `test_ws.py`       | Full sessions over a real WebSocket, error frames, backpressure, metrics |

The backpressure test pins the behaviour that matters: with a deliberately
slowed backend and a capacity-2 buffer, load is shed, the loss is reported in
the close frame and the meter, and the surviving audio is the newer audio.

`SessionTimer` takes an injectable clock, so latency arithmetic is asserted
exactly rather than with sleeps and tolerances.

CI runs the suite on Python 3.10–3.12, then boots the real server and drives it
with the demo client, then builds the Docker image and health-checks the
container.

## Design notes

**TTFB is measured from first audio, not socket open.** The client decides when
to start talking. Including that idle time makes the metric a measure of user
behaviour instead of system performance.

**Drops are counted, never silent.** `AudioBuffer.push` returns `False` when it
had to discard, the session counts it, the close frame reports it, and the meter
exposes it.

**Backend exceptions are translated at the boundary.** A vendor error becomes a
`backend_error` protocol frame. Letting a vendor exception reach the client
leaks implementation detail and gives the client nothing to branch on.

**Both the deployment contract and the backend capability are checked at
`start`.** They are different constraints, and a mismatch in either produces
garbage transcripts rather than an error — so both are validated before a single
byte of audio is accepted.

**Idle and wall-clock timeouts both exist.** A client that opens a socket, sends
`start`, and disappears must not hold a worker slot until the process restarts.

## Roadmap

- Real backend adapters (an HTTP/WebSocket vendor adapter as a worked example)
- Per-tenant rate limiting and concurrent-session quotas
- OpenTelemetry spans alongside the Prometheus counters
- Opus/WebM ingestion with server-side decode
- Session recording to object storage for offline evaluation

Pairs naturally with an offline evaluation harness: this gateway produces the
latency numbers, and a WER harness scores the transcripts it emits.

## License

MIT — see [LICENSE](LICENSE).
