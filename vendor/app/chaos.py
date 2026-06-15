"""Chaos transforms for the gh-archive-vendor.

All knobs read from env at module import. The vendor service re-imports on
restart so `make vendor-chaos` / `make vendor-calm` take effect immediately.

Five modes (all applied at the file-fetch boundary in main.py):

  * slow-file       — throttle response to ~50 KB/s
  * late-file       — keep a file 404 for LATE_FILE_DELAY_SECONDS past its
                       sim-hour boundary
  * truncated-file  — cut the stream after ~70% of bytes
  * schema-drift    — inject one extra payload field into one random event
  * outage          — return 503 for the configured sim-time windows
"""
from __future__ import annotations

import gzip
import io
import json
import os
import random
from datetime import datetime
from typing import Optional


SLOW_FILE_RATE = float(os.environ.get("VENDOR_SLOW_FILE_RATE", "0") or 0)
LATE_FILE_RATE = float(os.environ.get("VENDOR_LATE_FILE_RATE", "0") or 0)
LATE_FILE_DELAY_SECONDS = float(os.environ.get("VENDOR_LATE_FILE_DELAY_SECONDS", "60") or 60)
TRUNCATED_FILE_RATE = float(os.environ.get("VENDOR_TRUNCATED_FILE_RATE", "0") or 0)
SCHEMA_DRIFT = (os.environ.get("VENDOR_SCHEMA_DRIFT", "off") or "off").lower() in (
    "on",
    "1",
    "true",
    "yes",
)

SLOW_BYTES_PER_SEC = 50 * 1024     # ~50 KB/s when slow-file fires
TRUNCATE_FRACTION = 0.70           # serve this fraction of bytes when truncated


def _stable_rng(filename: str, salt: str) -> random.Random:
    """Per-(filename, mode) deterministic RNG — so the same file consistently
    trips or doesn't trip a given chaos mode within one process lifetime."""
    return random.Random(hash((filename, salt)) & 0xFFFFFFFF)


def should_slow(filename: str) -> Optional[int]:
    """Return target bytes-per-sec if this file should be served slowly, else None."""
    if SLOW_FILE_RATE <= 0:
        return None
    if _stable_rng(filename, "slow").random() < SLOW_FILE_RATE:
        return SLOW_BYTES_PER_SEC
    return None


def should_be_late(filename: str) -> bool:
    """Whether this file's 404→200 flip should be delayed past the sim boundary."""
    if LATE_FILE_RATE <= 0:
        return False
    return _stable_rng(filename, "late").random() < LATE_FILE_RATE


def should_truncate(filename: str) -> Optional[float]:
    """Return fraction-of-bytes to serve if this file should be truncated, else None."""
    if TRUNCATED_FILE_RATE <= 0:
        return None
    if _stable_rng(filename, "trunc").random() < TRUNCATED_FILE_RATE:
        return TRUNCATE_FRACTION
    return None


def inject_schema_drift(file_bytes: bytes) -> bytes:
    """Decompress, mutate one event's payload to add a synthetic field, recompress.

    Best-effort: if anything goes wrong, returns the original bytes unchanged
    (chaos must never crash the vendor).
    """
    if not SCHEMA_DRIFT:
        return file_bytes
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(file_bytes), mode="rb") as gz:
            raw = gz.read()
        lines = raw.split(b"\n")
        # Find the first non-empty line we can JSON-parse.
        for i, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                evt = json.loads(line)
            except Exception:
                continue
            payload = evt.get("payload")
            if not isinstance(payload, dict):
                payload = {}
                evt["payload"] = payload
            payload["crater_drift_marker"] = "schema-drift-injected"
            lines[i] = json.dumps(evt).encode("utf-8")
            break
        else:
            return file_bytes
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb", mtime=0) as gz:
            gz.write(b"\n".join(lines))
        return out.getvalue()
    except Exception as e:
        print(f"[chaos] schema-drift injection failed: {e}", flush=True)
        return file_bytes


# ───────────────────────── Outage windows ───────────────────────────────────

def parse_outage_schedule(raw: str | None) -> list[tuple[tuple[int, int], tuple[int, int]]]:
    """Parse 'HH:MM-HH:MM,HH:MM-HH:MM' into [( (h1,m1),(h2,m2) ), ...]."""
    if not raw:
        return []
    out: list[tuple[tuple[int, int], tuple[int, int]]] = []
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            a, b = piece.split("-", 1)
            ah, am = (int(x) for x in a.split(":", 1))
            bh, bm = (int(x) for x in b.split(":", 1))
            out.append(((ah, am), (bh, bm)))
        except Exception:
            continue
    return out


def in_outage(now: datetime, windows: list[tuple[tuple[int, int], tuple[int, int]]]) -> bool:
    cur = now.hour * 60 + now.minute
    for (h1, m1), (h2, m2) in windows:
        start = h1 * 60 + m1
        end = h2 * 60 + m2
        if start <= cur < end:
            return True
    return False
