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
	@echo "  === System Interfaces ==="
	@echo "  Jupyter Lab UI:   http://localhost:18888  (Token: devtoken)"
	@echo "  Kafka Dashboard:  http://localhost:28080"
	@echo "  Spark Live UI:    http://localhost:14040"
	@echo "  Neo4j Browser:    http://localhost:7474   (User: neo4j / Password: neo4jpassword)"
	@echo "  Vendor API Docs:  http://localhost:18400/docs"
	@echo "  Vendor Health:    http://localhost:18400/healthz"
	@echo "   Once gh-archive-vendor is healthy:"
	@echo "     curl http://localhost:18400/healthz"
	@echo "     curl -I http://localhost:18400/2024-01-15-0.json.gz"
	@echo ""

run:
	docker compose up -d --build
	@echo ""
	@echo "=============================================================="
	@echo " Crater vendor mock is starting."
	@echo "   First run downloads the configured GH Archive window."
	@echo "   Full 6-sim-day default = ~14 GB (~20-40 min). Watch progress:"
	@echo "     docker compose logs -f data-init"
	@echo ""
	@echo "   === Access Your Interfaces ==="
	@echo "   Jupyter Lab:  http://localhost:18888  (Token: devtoken)"
	@echo "   Kafka UI:     http://localhost:28080"
	@echo "   Neo4j Browser:http://localhost:7474"
	@echo "   Spark UI:     http://localhost:14040"
	@echo ""
	@echo "   Once gh-archive-vendor is healthy:"
	@echo "     curl http://localhost:18400/healthz"
	@echo "     curl -I http://localhost:18400/2024-01-15-0.json.gz"
	@echo "=============================================================="

run-full:
	make run
	make run-ingestor
	make run-spark


run-ingestor:
	@echo "🚀 Starting Ingestor for 30 seconds..."
	docker exec -d ingestor-python python main.py
	@sleep 30
	@echo "⏱️ 30 seconds reached! Stopping Ingestor gracefully..."
	-docker restart ingestor-python
	@echo "✅ Ingestion cycle finished."

stop:
	docker compose down --remove-orphans

reset:
	make reset-files
	docker compose down -v --remove-orphans
	-docker volume rm crater_streaming_data crater_streaming_checkpoints 2>/dev/null || true

restart:
	make reset
	make run

reset-data:
	make reset-dbs
	make reset-files
	
reset-files:
	@echo "🧹 Cleaning Spark Checkpoints & Parquet Data Lake..."
	@sudo rm -rf ./spark-code/checkpoints
	@sudo rm -rf ./spark-code/data_lake/events
	@echo "✨ System is 100% fresh, clean, and ready for a fresh run!"


reset-dbs:
	@echo "🛑 Stopping Neo4j and Postgres to release memory and locks..."
	@docker stop neo4j-server 
	
	@echo "🧼 ReStarting neo4j ..."
	@docker compose down -v neo4j
	@docker compose up -d neo4j
	@echo "⏳ Waiting for Neo4j to initialize (5 seconds)..."
	@sleep 10
	
	@echo "🏛️ Resetting Postgres Schema..."
	@docker exec -i postgres-server psql -U spark -d crater_analytics -c " \
		DROP SCHEMA IF EXISTS public CASCADE; \
		CREATE SCHEMA public; \
		GRANT ALL ON SCHEMA public TO public; \
	"

logs:
	docker compose logs -f gh-archive-vendor

logs-pipeline:
	docker compose logs -f ingestor-python spark-processor 

run-spark:
	@echo "🚀 Starting Spark Streaming Pipeline..."
	docker exec -it spark-processor /opt/spark/bin/spark-submit --master 'local[1]' --driver-memory 1g /app/code/pipeline.py

setup-dbs:
	make setup-neo4j
	make setup-postgres

setup-neo4j:
	@echo "🟢 Neo4j is up! Creating constraints..."
	docker exec -i neo4j-server cypher-shell -u neo4j -p neo4jpassword " \
		CREATE CONSTRAINT unique_actor IF NOT EXISTS FOR (a:Actor) REQUIRE a.login IS UNIQUE; \
		CREATE CONSTRAINT unique_repository IF NOT EXISTS FOR (r:Repository) REQUIRE r.name IS UNIQUE;"

setup-postgres:
	docker exec -it -e PGPASSWORD=spark postgres-server psql -U spark -d crater_analytics -c \
		"CREATE INDEX IF NOT EXISTS idx_pull_requests_repo_name ON pull_requests(repo_name); \
 		 CREATE INDEX IF NOT EXISTS idx_pushes_repo_pusher ON pushes (repo_name, actor_login) INCLUDE (push_id); \
		 CREATE INDEX IF NOT EXISTS idx_pushes_repo_author ON pushes (repo_name, commit_author_name, commit_author_email) WHERE commit_author_name IS NOT NULL;" \

serving:
	make setup-dbs
	@echo "🚀 Starting Spark Serving..."
	docker exec -it ingestor-python python src/serving.py

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
