# ─── Crater capstone project ────────────────────────────────────────────────
#
# Run from the project root. The scaffold ships only the upstream:
#   * data-init          — one-shot sidecar that downloads a window of
#                          gzipped hourly GH Archive JSONL files into a named
#                          docker volume. ~20-40 min on first run for the
#                          full 6-sim-day window (~14 GB). Idempotent — exits
#                          in <2s after that.
#   * gh-archive-vendor  — FastAPI service that serves
#                          GET /{YYYY-MM-DD-H}.json.gz gated by a simulated
#                          clock advancing at REPLAY_SECONDS_PER_HOUR pace.
#
# Everything else (probing, ingest, storage, normalisation, the analyst SQL
# surface) is yours to design. Add services to compose.yml as you need them.
# ────────────────────────────────────────────────────────────────────────────

.PHONY: run stop reset logs vendor-chaos vendor-calm help

help:
	@echo ""
	@echo "  make run            Build vendor image, run data-init, start gh-archive-vendor"
	@echo "  make stop           Stop containers (keeps the gh-archive-cache volume)"
	@echo "  make reset          Stop + wipe volumes (next run re-downloads the window)"
	@echo "  make logs           Tail gh-archive-vendor logs"
	@echo "  make vendor-chaos   Restart gh-archive-vendor with slow/late/truncated/drift/outage on"
	@echo "  make vendor-calm    Restart gh-archive-vendor with chaos all-off"
	@echo ""
	@echo "  Vendor API:  http://localhost:18400/docs"
	@echo "  Healthcheck: http://localhost:18400/healthz"
	@echo ""

run:
	docker compose up -d --build
	@echo ""
	@echo "=============================================================="
	@echo " Crater vendor mock is starting."
	@echo "   First run downloads the configured GH Archive window."
	@echo "   Full 6-sim-day default = ~14 GB (~20-40 min). Watch progress:"
	@echo "     docker compose logs -f data-init"
	@echo "   Once gh-archive-vendor is healthy:"
	@echo "     curl http://localhost:18400/healthz"
	@echo "     curl -I http://localhost:18400/2024-01-15-0.json.gz"
	@echo "=============================================================="

stop:
	docker compose down --remove-orphans

reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f gh-archive-vendor

vendor-chaos:
	VENDOR_SLOW_FILE_RATE=0.10 \
	VENDOR_LATE_FILE_RATE=0.15 \
	VENDOR_LATE_FILE_DELAY_SECONDS=20 \
	VENDOR_TRUNCATED_FILE_RATE=0.10 \
	VENDOR_SCHEMA_DRIFT=on \
	VENDOR_OUTAGE_SCHEDULE=03:00-03:02 \
	docker compose up -d --no-deps --force-recreate gh-archive-vendor
	@echo "[chaos] gh-archive-vendor restarted with slow/late/truncated/drift/outage on."

vendor-calm:
	VENDOR_SLOW_FILE_RATE=0 \
	VENDOR_LATE_FILE_RATE=0 \
	VENDOR_TRUNCATED_FILE_RATE=0 \
	VENDOR_SCHEMA_DRIFT=off \
	VENDOR_OUTAGE_SCHEDULE= \
	docker compose up -d --no-deps --force-recreate gh-archive-vendor
	@echo "[calm] gh-archive-vendor restarted with chaos disabled."
