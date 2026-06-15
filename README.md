# Crater — talent intelligence for engineering leaders (capstone E)

Your job is to design the system. **This scaffold only ships the upstream
vendor mock and the dataset bootstrap.** Everything else — probing, ingest,
polymorphic normalisation, storage, the analyst SQL surface — is yours.

**Start here:** [`BRIEF.md`](./BRIEF.md).

## Bring it up

```bash
make run
```

> **Storage warning.** The default replay window is **6 sim-days = 144
> hourly files ≈ 14 GB gzipped on disk** (~34M events). The first run
> downloads all of them from `data.gharchive.org` and dominates wall-time
> — expect **20-40 minutes** on a typical home connection. Subsequent
> runs reuse the docker volume (`gh-archive-cache`) and start in seconds.
> `make reset` wipes the volume and forces a re-download.
>
> If you're just kicking the tyres, set `MAX_FILES_TO_DOWNLOAD=6` in
> `.env` before the first `make run` — that caps the cache at ~600 MB
> and ~1-2 min of download, enough to verify the contract end-to-end.

Watch progress:

```bash
docker compose logs -f data-init
```

## What's running

| Service              | Purpose                                                                | URL                          |
|----------------------|------------------------------------------------------------------------|------------------------------|
| `data-init`          | One-shot: downloads the configured window of hourly GH Archive files.  | (no port; exits when done)   |
| `gh-archive-vendor`  | FastAPI mock. Serves `GET /{YYYY-MM-DD-H}.json.gz` gated by sim clock. | http://localhost:18400       |

Useful endpoints on `gh-archive-vendor`:

- `GET /healthz` — `{ready, simulated_now, files_available}`
- `GET /simulated_now` — `{simulated_now}` (in ISO-8601 UTC)
- `GET /stats` — request counters + current chaos config
- `GET /docs` — OpenAPI / Swagger UI
- `GET /{YYYY-MM-DD-H}.json.gz` — the hourly file (200 once sim clock passes)

### Sample calls

```bash
# Health
curl -s http://localhost:18400/healthz | jq

# Where is the simulated clock right now?
curl -s http://localhost:18400/simulated_now | jq

# Probe the first hour. 404 until the sim clock has passed 2024-01-15T01:00Z;
# 200 thereafter. Default replay = 2 wall-sec per sim-hour, so within a few
# seconds of `make run` finishing data-init you should see 200 here.
curl -I http://localhost:18400/2024-01-15-0.json.gz

# Sniff an event from the first hour. The hour segment is NOT zero-padded.
curl -s http://localhost:18400/2024-01-15-0.json.gz | gunzip | head -1 | jq .type
```

The hour segment in the URL is **not zero-padded** —
`2024-01-15-3.json.gz` for 03:00, `2024-01-15-13.json.gz` for 13:00.
This mirrors the real `data.gharchive.org` convention.

## Chaos toggle

The vendor can simulate the failure modes enumerated in `BRIEF.md` (slow
files, late files, truncated files, schema drift, outages).

```bash
make vendor-chaos   # turn it on
make vendor-calm    # turn it off
```

Knobs live in `.env` (see comments in `.env.example` for what each does).
Restart `gh-archive-vendor` after editing `.env`:

```bash
docker compose restart gh-archive-vendor
```

## Replay speed

Default `REPLAY_SECONDS_PER_HOUR=2` means one simulated hour per 2
wall-clock seconds — the full 6-sim-day window completes in roughly five
wall-minutes. Slow it down (5-10 wall-sec per sim-hour) for relaxed
debugging; speed it up for stress tests.

## Adding your own services

The scaffold deliberately ships a thin `compose.yml`. Add services as you
need them — copy snippets from the course's `labs/streaming/compose.yml`,
`labs/monitoring/compose.yml`, etc. There's no opinionated module layout
under `src/` — design as a team and make your choices defensible.

## Editing in VS Code

The course already has a remote-development pattern (see
`scaffolds/streaming-clickstream/README.md`). Add your own
`.devcontainer/devcontainer.json` pointing at whichever container you spin
up for development.

## Data notes (read me before defending)

The vendor serves the **real** GH Archive bytes for the configured window
— no synthesis, no enrichment. Everything you see is what was published
on those hours of those days. That means:

- **Bots are present.** A lot of the high-volume activity in any GH
  Archive hour comes from automation accounts (Dependabot, GitHub
  Actions, release bots). Decide how you want to handle them; some
  questions in the brief get noisier answers if you don't.
- **Repositories appear and disappear.** A repo's `language` can change
  across the window, and a repo can be deleted (which shows up as a
  `DeleteEvent` and then absence). Decide whether you treat the
  repository identity as the numeric `repo.id` or the human-readable
  `repo.name` — the answers to question 2 in particular depend on it.
- **Force-pushes rewrite history.** A `PushEvent` can carry a `forced`
  flag; the commits listed under a forced push are the ones that *now*
  point at HEAD, not the ones the pusher actually authored. Account for
  this when you count commit authors in question 2.
- **The window is six days.** Some signals (question 4's
  star→fork→PR funnel) need their look-forward windows to fit inside
  what's left of the replay; the brief's `2 sim-days` and `5 sim-days`
  thresholds are tuned for this.
