# Crater — Talent Intelligence for Engineering Leaders

A production-grade, chaos-resilient enterprise data engineering pipeline designed to ingest, normalize, and analyze high-velocity GitHub event streams for real-time telemetry and talent analytics.

## 🚀 Architecture & Design Principles (System Guarantees)

Our system is built to provide reliable insights from high-velocity GitHub event streams. We adhere to three core design principles:

* **At-Least-Once Delivery:** To prevent data loss, the pipeline is architected with Kafka as a resilient buffer, utilizing Spark Structured Streaming checkpoints and robust retry logic. Every event is captured and persisted.

* **Read-Time Deduplication:** We chose an "At-least-once" ingestion model to maximize availability and throughput. Data integrity is maintained at the Query Layer using DISTINCT and GROUP BY operations, ensuring accurate analytics even if retries produce duplicate records.

* **Horizontal Scalability:** By decoupling ingestion from processing using Kafka, the system can scale independently. Spark workers can be added dynamically to handle traffic spikes, ensuring performance under heavy load.

## 🛠 Project Overview

A custom implementation covering probing, ingestion, polymorphic normalization, and storage design for complex GH Archive event streams. Start here: [BRIEF.md](./BRIEF.md).

## 🏗 Storage & Analytics Rationale

| Storage | Purpose | 
| ----- | ----- | 
| **Data Lake (Parquet)** | Immutable raw event audit trail. | 
| **Analytics (Postgres)** | Complex aggregations & funnel metrics (Q1, Q2, Q4). | 
| **Graph (Neo4j)** | Relationship & collaboration patterns (Q3, Q5). | 

* **Identity Management:** We rely on repo.id as the primary key for repository identity to handle naming inconsistencies, while using human-readable names for reporting.

* **Idempotency:** All write operations are idempotent—Neo4j uses MERGE, and Postgres queries are structured to be insensitive to duplicate event IDs.

## 🛡️ Chaos & Resilience Matrix

Our ingestion pipeline is custom-built to automatically survive and self-heal from all 5 simulated chaos modes with zero human intervention:

| Chaos Mode | Simulated Behavior | System Resilience & Mitigation Strategy | 
| ----- | ----- | ----- | 
| **Outage (503)** | Simulated upstream downtime windows. | Detects non-200 status, raises an exception, and enters a **robust Exponential Backoff** retry loop (2s, 4s, 8s, 16s, 32s) to prevent API spamming while waiting for recovery. | 
| **Late File (404)** | Scheduled file delayed past simulated boundary. | Detects "404" explicitly. Instead of consuming the network retry budget, it triggers a **silent 5-second wait** and loops back (`continue`), preserving retry pools for true failures. | 
| **Slow File (Timeout)** | Download speed throttled to ~50 KB/s. | Implements a connection/read split timeout: `timeout=(15, None)`. Connects fast, but **reads patiently without timing out**, ensuring slow blocks finish downloading. | 
| **Truncated File** | Gzip payload abruptly cut off mid-stream. | Processes streams within a transaction-like `try-except` block. Any decompression (`BadGzipFile`) or JSON parsing error immediately **discards the partial block, freezes the Watermark**, and schedules a complete re-fetch. | 
| **Schema Drift** | Injected unexpected synthetic metadata fields. | Preserves the entire event payload as a **raw JSON String in the Parquet Data Lake**. Downstream Postgres and Neo4j consumers extract only defined schema keys, silently ignoring drift markers. | 

### 🛡️ Resilience in Action (Actual Execution Logs)

Below is an extract from our live `/tmp/ingestor_history.jsonl` demonstrating self-healing behavior under both simulation-clock delays (404) and sudden network failures (Read Timeout):

```
{"timestamp": "2026-06-25T14:51:16.586509", "filename": "2024-01-15-0.json.gz", "status": "FAILURE", "error": "Upstream returned status code: 404"}
{"timestamp": "2026-06-25T14:51:21.590734", "filename": "2024-01-15-0.json.gz", "status": "FAILURE", "error": "Upstream returned status code: 404"}
{"timestamp": "2026-06-25T15:11:35.910569", "filename": "2024-01-15-0.json.gz", "status": "SUCCESS", "error": null}
{"timestamp": "2026-06-25T15:17:10.021929", "filename": "2024-01-15-1.json.gz", "status": "SUCCESS", "error": null}
{"timestamp": "2026-06-25T15:17:41.291515", "filename": "2024-01-15-2.json.gz", "status": "FAILURE", "error": "HTTPConnectionPool(host='gh-archive-vendor', port=8000): Read timed out. (read timeout=30)"}
{"timestamp": "2026-06-25T15:18:13.480700", "filename": "2024-01-15-2.json.gz", "status": "FAILURE", "error": "HTTPConnectionPool(host='gh-archive-vendor', port=8000): Read timed out. (read timeout=30)"}
```

> **Storage warning:** The initial 6-day data download requires ~20-40 minutes and ~14 GB of disk space. Subsequent runs use cached volumes.

## 📈 Monitoring & Chaos

* **Health:** `curl -s http://localhost:18400/healthz | jq`

* **Chaos:** * `make vendor-chaos` (Enable)

  * `make vendor-calm` (Disable)

## 📝 Defence Notes

* **Bot Filtering:** Known automation accounts (e.g., dependabot, renovate) are filtered out to focus on human engineering impact.

* **Force-pushes:** PushEvent logic accounts for the forced flag in PushEvent to ensure commit counts reflect the current repository history, not just the raw push records.
