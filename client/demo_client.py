#!/usr/bin/env python3
"""Streaming demo client.

Streams synthetic PCM to a running gateway at real-time pace and reports the
latency the *client* observed, which is the number that matters: server-side
timing cannot see network or scheduling delay on the receiving end.

    python client/demo_client.py --seconds 3
    python client/demo_client.py --url ws://localhost:8000/v1/stream --file audio.raw

Requires `websockets` (installed with the dev extra).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import struct
import sys
import time
from pathlib import Path

try:
    import websockets
except ImportError:  # pragma: no cover
    sys.exit("this demo needs `websockets`: pip install websockets")

CHUNK_MS = 20


def synthetic_pcm(seconds: float, sample_rate: int) -> bytes:
    """A quiet 220 Hz tone. Content is irrelevant to the mock backend; this just
    produces a realistically sized byte stream to pace the session."""
    total = int(seconds * sample_rate)
    return b"".join(
        struct.pack("<h", int(3000 * math.sin(2 * math.pi * 220 * n / sample_rate)))
        for n in range(total)
    )


def chunk_bytes(data: bytes, size: int):
    for offset in range(0, len(data), size):
        yield data[offset : offset + size]


async def run(args: argparse.Namespace) -> int:
    if args.file:
        audio = Path(args.file).read_bytes()
    else:
        audio = synthetic_pcm(args.seconds, args.sample_rate)

    bytes_per_chunk = int(args.sample_rate * 2 * CHUNK_MS / 1000)
    chunks = list(chunk_bytes(audio, bytes_per_chunk))

    print(f"connecting to {args.url}")
    async with websockets.connect(args.url) as ws:
        await ws.send(json.dumps({
            "type": "start",
            "sample_rate": args.sample_rate,
            "encoding": "linear16",
            "channels": 1,
        }))
        ready = json.loads(await ws.recv())
        if ready.get("type") != "ready":
            print(f"server refused the session: {ready}")
            return 1
        print(f"session {ready['session_id']} ready on backend {ready['backend']}\n")

        first_audio_at: float | None = None
        first_transcript_at: float | None = None
        finals: list[str] = []

        async def receive() -> None:
            nonlocal first_transcript_at
            async for raw in ws:
                frame = json.loads(raw)
                kind = frame.get("type")
                if kind in ("partial", "final") and first_transcript_at is None:
                    first_transcript_at = time.perf_counter()
                if kind == "partial":
                    print(f"  ... {frame['text']}", end="\r", flush=True)
                elif kind == "final":
                    finals.append(frame["text"])
                    print(f"  >>> {frame['text']}" + " " * 20)
                elif kind == "error":
                    print(f"\nerror [{frame['code']}]: {frame['message']}")
                    return
                elif kind == "closed":
                    print("\nserver-side summary:")
                    for key in ("duration_ms", "ttfb_ms", "chunks_received", "chunks_dropped"):
                        print(f"  {key:18} {frame.get(key)}")
                    return

        receiver = asyncio.create_task(receive())

        # Pace the send loop to wall-clock time. Blasting the whole file as fast
        # as the socket allows measures the buffer, not the latency a speaker
        # would actually experience.
        for chunk in chunks:
            if first_audio_at is None:
                first_audio_at = time.perf_counter()
            await ws.send(chunk)
            await asyncio.sleep(CHUNK_MS / 1000)

        await ws.send(json.dumps({"type": "stop"}))
        await receiver

    if first_audio_at and first_transcript_at:
        print(f"\nclient-observed TTFB: {(first_transcript_at - first_audio_at) * 1000:.1f} ms")
    print(f"transcript: {' '.join(finals)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream audio to the gateway and print transcripts.")
    parser.add_argument("--url", default="ws://localhost:8000/v1/stream")
    parser.add_argument("--seconds", type=float, default=3.0, help="length of synthetic audio")
    parser.add_argument("--sample-rate", type=int, default=16000)
    parser.add_argument("--file", help="raw 16-bit PCM file to stream instead of a tone")
    args = parser.parse_args()
    try:
        return asyncio.run(run(args))
    except (ConnectionRefusedError, OSError):
        print(f"could not connect to {args.url} - is the gateway running?")
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
