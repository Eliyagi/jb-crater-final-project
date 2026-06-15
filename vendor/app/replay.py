"""Simulated clock for the gh-archive-vendor.

Wall-time anchored: every tick, compute where `simulated_now` should be based
on real elapsed time and advance it. No subscriber fan-out, no slice queues —
the vendor is pull-based; the only thing the clock does is decide which
hourly files are "available" yet.

Persists state to <DATA_DIR>/_replay_state.json every 5 wall-seconds so
`docker compose restart` resumes mid-window instead of restarting from t=0.
"""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SECONDS_PER_HOUR = float(os.environ.get("REPLAY_SECONDS_PER_HOUR", "2"))
WINDOW_START = datetime.fromisoformat(
    os.environ.get("REPLAY_WINDOW_START", "2024-01-15")
).replace(tzinfo=timezone.utc)
WINDOW_END = datetime.fromisoformat(
    os.environ.get("REPLAY_WINDOW_END", "2024-01-21")
).replace(tzinfo=timezone.utc)

STATE_PATH = Path(os.environ.get("DATA_DIR", "/cache")) / "_replay_state.json"


@dataclass
class ReplayState:
    simulated_now: datetime = field(default_factory=lambda: WINDOW_START)
    requests_total: int = 0
    files_served: int = 0

    def snapshot(self) -> dict[str, Any]:
        return {
            "simulated_now": self.simulated_now.isoformat(),
            "requests_total": self.requests_total,
            "files_served": self.files_served,
        }

    def restore(self, payload: dict[str, Any]) -> None:
        try:
            self.simulated_now = datetime.fromisoformat(payload["simulated_now"])
            if self.simulated_now.tzinfo is None:
                self.simulated_now = self.simulated_now.replace(tzinfo=timezone.utc)
            self.requests_total = int(payload.get("requests_total", 0))
            self.files_served = int(payload.get("files_served", 0))
        except Exception:
            pass


def load_persisted() -> dict[str, Any] | None:
    if not STATE_PATH.exists():
        return None
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return None


async def replay_loop(state: ReplayState) -> None:
    """Advance `simulated_now` at the configured wall-clock pace.

    Re-anchors after each restore so resumed sessions don't fast-forward.
    Stops advancing once WINDOW_END is reached.
    """
    import time as _time

    TICK_S = 0.1
    sim_per_wall_s = 3600.0 / SECONDS_PER_HOUR  # simulated seconds per wall second

    if state.simulated_now < WINDOW_START:
        state.simulated_now = WINDOW_START

    wall_origin = _time.monotonic()
    sim_origin = state.simulated_now

    while True:
        wall_elapsed = _time.monotonic() - wall_origin
        target = sim_origin + timedelta(seconds=wall_elapsed * sim_per_wall_s)
        if target > WINDOW_END:
            target = WINDOW_END
        if target > state.simulated_now:
            state.simulated_now = target
        await asyncio.sleep(TICK_S)


async def persist_loop(state: ReplayState) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            STATE_PATH.write_text(json.dumps(state.snapshot(), indent=2))
        except Exception:
            pass
