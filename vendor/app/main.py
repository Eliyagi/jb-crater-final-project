"""FastAPI app for the gh-archive-vendor mock.

Pull-based contract — no subscriptions, no webhooks, no manifest.
Students compute the next URL from their own high-water mark and probe.

Routes:
  GET /healthz              — { ready, simulated_now, files_available }
  GET /stats                — counters + current chaos config
  GET /simulated_now        — { simulated_now }
  GET /{YYYY-MM-DD-H}.json.gz
        404 — sim clock has not reached that hour yet
        503 — outage window in effect
        200 — gzipped JSONL stream (possibly chaos-mangled)
"""
from __future__ import annotations

import asyncio
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Path as PathParam
from fastapi.responses import JSONResponse, StreamingResponse

from . import chaos
from . import replay as replay_mod


DATA_DIR = Path(os.environ.get("DATA_DIR", "/cache"))
READY_SENTINEL = DATA_DIR / "_READY"

FILENAME_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})-(\d{1,2})\.json\.gz$")

# Read once at startup; restart picks up changes (matches Lume's pattern).
OUTAGE_WINDOWS = chaos.parse_outage_schedule(os.environ.get("VENDOR_OUTAGE_SCHEDULE"))

# Per-process arrival deadline for late-file chaos. Maps filename → wall-time
# (monotonic) at which the file is allowed to flip from 404 to 200. Populated
# lazily on first request *after* the file's sim-hour boundary passes, so the
# late-by behaviour is "stays 404 for N extra wall-seconds past the boundary".
_late_deadlines: dict[str, float] = {}


# ───────────────────────── App state + lifespan ─────────────────────────────

state = replay_mod.ReplayState()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    persisted = replay_mod.load_persisted()
    if persisted:
        state.restore(persisted)

    replay_task = asyncio.create_task(replay_mod.replay_loop(state))
    persist_task = asyncio.create_task(replay_mod.persist_loop(state))
    try:
        yield
    finally:
        replay_task.cancel()
        persist_task.cancel()


app = FastAPI(title="Crater gh-archive-vendor mock", version="0.1.0", lifespan=lifespan)


# ───────────────────────── Helpers ──────────────────────────────────────────

def _sim_now_iso() -> str:
    return state.simulated_now.isoformat().replace("+00:00", "Z")


def _files_available_count() -> int:
    """How many cached hourly files exist on disk."""
    if not DATA_DIR.exists():
        return 0
    return sum(1 for p in DATA_DIR.iterdir() if FILENAME_RE.match(p.name))


def _parse_hour(filename: str) -> datetime | None:
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    y, mo, d, h = (int(x) for x in m.groups())
    try:
        return datetime(y, mo, d, h, tzinfo=timezone.utc)
    except ValueError:
        return None


# ───────────────────────── Routes ───────────────────────────────────────────

@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {
        "ready": READY_SENTINEL.exists(),
        "simulated_now": _sim_now_iso(),
        "files_available": _files_available_count(),
    }


@app.get("/simulated_now")
def simulated_now() -> dict[str, Any]:
    return {"simulated_now": _sim_now_iso()}


@app.get("/stats")
def stats() -> dict[str, Any]:
    return {
        "simulated_now": _sim_now_iso(),
        "files_served": state.files_served,
        "requests_total": state.requests_total,
        "files_available": _files_available_count(),
        "chaos": {
            "slow_file_rate": chaos.SLOW_FILE_RATE,
            "late_file_rate": chaos.LATE_FILE_RATE,
            "late_file_delay_seconds": chaos.LATE_FILE_DELAY_SECONDS,
            "truncated_file_rate": chaos.TRUNCATED_FILE_RATE,
            "schema_drift": chaos.SCHEMA_DRIFT,
            "outage_windows": [
                f"{a[0]:02d}:{a[1]:02d}-{b[0]:02d}:{b[1]:02d}"
                for a, b in OUTAGE_WINDOWS
            ],
        },
    }


@app.api_route("/{filename}", methods=["GET", "HEAD"])
async def get_file(filename: str = PathParam(..., description="YYYY-MM-DD-H.json.gz")) -> Any:
    state.requests_total += 1

    hour = _parse_hour(filename)
    if hour is None:
        raise HTTPException(status_code=404, detail="not found")

    sim_now = state.simulated_now
    hour_end = hour + timedelta(hours=1)

    # 1. Sim clock has not yet reached this hour's *end* → file doesn't exist yet.
    #    (Real GH Archive only publishes an hour's file after that hour closes.)
    if sim_now < hour_end:
        raise HTTPException(status_code=404, detail="hour not reached yet")

    # 2. Outage window in effect → 503.
    if chaos.in_outage(sim_now, OUTAGE_WINDOWS):
        return JSONResponse({"detail": "vendor outage"}, status_code=503)

    # 3. Late-file chaos: hold this file at 404 for N wall-seconds past the boundary.
    if chaos.should_be_late(filename):
        import time as _t
        deadline = _late_deadlines.get(filename)
        if deadline is None:
            deadline = _t.monotonic() + chaos.LATE_FILE_DELAY_SECONDS
            _late_deadlines[filename] = deadline
        if _t.monotonic() < deadline:
            raise HTTPException(status_code=404, detail="file late (chaos)")

    # 4. File must exist on disk.
    path = DATA_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="file not in cache")

    file_bytes = path.read_bytes()

    # 5. Schema-drift chaos: mutate before serving (no-op when disabled).
    file_bytes = chaos.inject_schema_drift(file_bytes)

    # 6. Truncated-file chaos: cut the payload.
    truncate_to = chaos.should_truncate(filename)
    if truncate_to is not None:
        cut = max(1, int(len(file_bytes) * truncate_to))
        file_bytes = file_bytes[:cut]

    # 7. Slow-file chaos: throttle the stream.
    throttle_bps = chaos.should_slow(filename)

    state.files_served += 1

    async def _iter():
        if throttle_bps is None:
            # Single chunk — small files stream fast.
            yield file_bytes
            return
        chunk = 8 * 1024
        per_chunk_sleep = chunk / throttle_bps
        for i in range(0, len(file_bytes), chunk):
            yield file_bytes[i : i + chunk]
            await asyncio.sleep(per_chunk_sleep)

    headers = {"content-length": str(len(file_bytes))}
    # When throttled, omit content-length so the client doesn't pre-allocate
    # and so a short read is unambiguous on the wire.
    if throttle_bps is not None:
        headers.pop("content-length", None)
    return StreamingResponse(
        _iter(),
        media_type="application/gzip",
        headers=headers,
    )
