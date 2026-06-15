#!/usr/bin/env python3
"""data-init: download a window of GH Archive hourly files into /cache.

GH Archive serves one gzipped JSONL file per hour at the predictable URL
pattern `https://data.gharchive.org/{YYYY-MM-DD-H}.json.gz` (hour is NOT
zero-padded — e.g. `2024-01-15-3.json.gz`). Each file holds all public
GitHub events for that hour: typically 150k-300k events, ~50-200 MB
gunzipped, ~30-100 MB gzipped on disk.

Behaviour:
  * Walks every hour in [REPLAY_WINDOW_START, REPLAY_WINDOW_END) — start
    inclusive, end exclusive (so REPLAY_WINDOW_END=2024-01-21 with start
    2024-01-15 gives 6 sim-days = 144 hours).
  * For each hour, downloads to `/cache/{YYYY-MM-DD-H}.json.gz` if absent.
    Idempotent: existing files are skipped, so a re-run after `make stop` is
    a no-op; `make reset` wipes the volume and forces a re-download.
  * Optional `MAX_FILES_TO_DOWNLOAD` env var caps the number of files
    fetched (truncates the window). Critical for smoke tests — set to a
    small integer (e.g. 6) to download ~600 MB in ~1-2 min instead of the
    default ~14 GB / ~20-40 min.
  * Concurrency: 4-way parallel downloads via asyncio + httpx.
  * Retries 429 / 5xx with exponential backoff.
  * Validates Content-Length matches written bytes (catches truncation
    mid-download).
  * Writes a `_READY` sentinel last so the vendor service can refuse to
    start serving from a half-populated cache.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx


GH_ARCHIVE_BASE = "https://data.gharchive.org"
CONCURRENCY = 4
RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)


def _parse_date(raw: str) -> date:
    return datetime.fromisoformat(raw).date()


def _filename_for(hour: datetime) -> str:
    # Hour NOT zero-padded — GH Archive's convention.
    return f"{hour.year:04d}-{hour.month:02d}-{hour.day:02d}-{hour.hour}.json.gz"


def _hours_in_window(start: date, end: date) -> list[datetime]:
    """Every hour in [start 00:00, end 00:00) — end is exclusive."""
    out: list[datetime] = []
    cur = datetime(start.year, start.month, start.day)
    stop = datetime(end.year, end.month, end.day)
    while cur < stop:
        out.append(cur)
        cur += timedelta(hours=1)
    return out


async def _fetch_one(
    client: httpx.AsyncClient,
    hour: datetime,
    cache_dir: Path,
    sem: asyncio.Semaphore,
) -> tuple[str, int, str]:
    """Returns (filename, bytes_written, status) where status ∈ {'ok','skip','fail'}."""
    filename = _filename_for(hour)
    target = cache_dir / filename
    if target.exists() and target.stat().st_size > 0:
        return filename, target.stat().st_size, "skip"

    url = f"{GH_ARCHIVE_BASE}/{filename}"
    tmp = target.with_suffix(target.suffix + ".part")

    async with sem:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with client.stream("GET", url) as resp:
                    if resp.status_code in RETRY_STATUSES:
                        raise httpx.HTTPStatusError(
                            f"retryable {resp.status_code}", request=resp.request, response=resp
                        )
                    resp.raise_for_status()
                    expected = resp.headers.get("content-length")
                    written = 0
                    with tmp.open("wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=1 << 16):
                            f.write(chunk)
                            written += len(chunk)
                    if expected is not None and int(expected) != written:
                        tmp.unlink(missing_ok=True)
                        raise RuntimeError(
                            f"short read: got {written} bytes, expected {expected}"
                        )
                    tmp.replace(target)
                    return filename, written, "ok"
            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"[data-init]   {filename}: FAILED after {attempt}x — {e}", flush=True)
                    tmp.unlink(missing_ok=True)
                    return filename, 0, "fail"
                backoff = min(2 ** attempt, 30)
                print(
                    f"[data-init]   {filename}: attempt {attempt} failed ({e}); retrying in {backoff}s",
                    flush=True,
                )
                await asyncio.sleep(backoff)

    return filename, 0, "fail"


async def _run(cache_dir: Path) -> int:
    start = _parse_date(os.environ.get("REPLAY_WINDOW_START", "2024-01-15"))
    end = _parse_date(os.environ.get("REPLAY_WINDOW_END", "2024-01-21"))
    max_files_raw = os.environ.get("MAX_FILES_TO_DOWNLOAD", "").strip()
    max_files = int(max_files_raw) if max_files_raw else None

    cache_dir.mkdir(parents=True, exist_ok=True)
    ready_path = cache_dir / "_READY"
    # Remove a stale sentinel so a partial previous run doesn't fool the vendor.
    ready_path.unlink(missing_ok=True)

    hours = _hours_in_window(start, end)
    if max_files is not None:
        hours = hours[:max_files]

    print(
        f"[data-init] window {start} → {end} = {len(_hours_in_window(start, end))} hours; "
        f"fetching {len(hours)} (MAX_FILES_TO_DOWNLOAD={max_files_raw or 'unset'})",
        flush=True,
    )

    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        tasks = [_fetch_one(client, h, cache_dir, sem) for h in hours]
        results = await asyncio.gather(*tasks)

    ok = [r for r in results if r[2] == "ok"]
    skipped = [r for r in results if r[2] == "skip"]
    failed = [r for r in results if r[2] == "fail"]
    total_bytes = sum(r[1] for r in ok) + sum(r[1] for r in skipped)

    print(
        f"[data-init] done: {len(ok)} downloaded, {len(skipped)} already-present, "
        f"{len(failed)} failed; cache total ~{total_bytes / (1 << 20):.1f} MiB",
        flush=True,
    )

    if failed:
        print(f"[data-init] aborting — {len(failed)} files failed", flush=True)
        return 1

    ready_path.write_text(
        f"window_start={start}\nwindow_end={end}\nfiles={len(hours)}\nbytes={total_bytes}\n"
    )
    print(f"[data-init] wrote {ready_path}", flush=True)
    return 0


def main() -> int:
    cache_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "/cache").resolve()
    return asyncio.run(_run(cache_dir))


if __name__ == "__main__":
    sys.exit(main())
