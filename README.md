# Crater — Talent Intelligence for Engineering Leaders

## 🚀 Architecture & Design Principles (System Guarantees)

Our system is built to provide reliable insights from high-velocity GitHub event streams. We adhere to three core design principles:

* **At-Least-Once Delivery:** To prevent data loss, the pipeline is architected with Kafka as a resilient buffer, utilizing Spark Structured Streaming checkpoints and robust retry logic. Every event is captured and persisted.

* **Read-Time Deduplication:** We chose an "At-least-once" ingestion model to maximize availability and throughput. Data integrity is maintained at the Query Layer using DISTINCT and GROUP BY operations, ensuring accurate analytics even if retries produce duplicate records.

* **Horizontal Scalability:** By decoupling ingestion from processing using Kafka, the system can scale independently. Spark workers can be added dynamically to handle traffic spikes, ensuring performance under heavy load.

## 🛠 Project Overview

A custom implementation covering probing, ingestion, polymorphic normalization, and storage design for complex GH Archive event streams. Start here: [BRIEF.md](./BRIEF.md).

## 🏗 Storage & Analytics Rationale

| Storage | Purpose |
| :--- | :--- |
| **Data Lake (Parquet)** | Immutable raw event audit trail. |
| **Analytics (Postgres)** | Complex aggregations & funnel metrics (Q1, Q2, Q4). |
| **Graph (Neo4j)** | Relationship & collaboration patterns (Q3). |

* **Identity Management:** We rely on repo.id as the primary key for repository identity to handle naming inconsistencies, while using human-readable names for reporting.

* **Idempotency:** All write operations are idempotent—Neo4j uses MERGE, and Postgres queries are structured to be insensitive to duplicate event IDs.

## ⚙️ Usage

```bash
make run
```

> **Storage warning:** The initial 6-day data download requires ~20-40 minutes and ~14 GB of disk space. Subsequent runs use cached volumes.

## 📈 Monitoring & Chaos

* **Health:** `curl -s http://localhost:18400/healthz | jq`

* **Chaos:** * `make vendor-chaos` (Enable)
  * `make vendor-calm` (Disable)

## 📝 Defence Notes

* **Bot Filtering:** Known automation accounts (e.g., dependabot, renovate) are filtered out to focus on human engineering impact.

* **Force-pushes:** PushEvent logic accounts for the forced flag in PushEvent to ensure commit counts reflect the current repository history, not just the raw push records.