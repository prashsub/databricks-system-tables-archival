# Architecture Overview

## Purpose

The System Tables Archival pipeline preserves Databricks System Table data indefinitely, beyond the rolling retention windows (30-365 days depending on the table). Without archival, historical data is silently deleted by Delta Sharing VACUUM operations on the source tables.

## System Architecture

```
+-----------------------------------------------------------------------+
|                      Databricks Workspace                              |
|                                                                        |
|  +--------------------------+                                          |
|  | system.* (Source Tables) |  <-- Rolling retention (30-365 days)     |
|  | Delivered via Delta      |  <-- VACUUM after 7 days                 |
|  | Sharing                  |                                          |
|  +--------+-----------------+                                          |
|           |                                                            |
|           | Daily triggered workflow (2am UTC)                         |
|           |                                                            |
|  +--------v---------------------------------------------------------+  |
|  | Workflow: System Tables - Ingest Archive                          |  |
|  |                                                                   |  |
|  |  +------------------+  +------------------+  +------------------+ |  |
|  |  | Task 1: SDP      |  | Task 2: Dedup    |  | Task 3: Batch    | |  |
|  |  | Pipeline         |->| Streaming Sinks  |->| Companion        | |  |
|  |  |                  |  |                  |  |                  | |  |
|  |  | 27 streaming     |  | 27 tables        |  | 4 watermark MERGE| |  |
|  |  | tables via       |  | Skip if clean    |  | 6 full overwrite | |  |
|  |  | append_flow +    |  | INSERT OVERWRITE |  | Serverless       | |  |
|  |  | Delta sinks      |  | if dupes found   |  |                  | |  |
|  |  +------------------+  +------------------+  +------------------+ |  |
|  +-------------------------------------------------------------------+  |
|           |                                                            |
|  +--------v---------------------------------------------------------+  |
|  | ${target_catalog} (Persistent Archive)                            |  |
|  |                                                                   |  |
|  | 12 schemas, 37 tables                                             |  |
|  | CLUSTER BY AUTO + Predictive Optimization enabled                 |  |
|  | No retention limit -- data preserved indefinitely                 |  |
|  +-------------------------------------------------------------------+  |
|                                                                        |
|  +--------------------------+     +--------------------------+         |
|  | Freshness Check Job      |     | One-Time Setup Job       |         |
|  | Daily 8am UTC            |     | Manual trigger only      |         |
|  | Alerts if >48h stale     |     | Creates catalog/schemas  |         |
|  +--------------------------+     +--------------------------+         |
+-----------------------------------------------------------------------+
```

## Jobs

| Job | Schedule | Purpose |
|-----|----------|---------|
| **System Tables - Ingest Archive** | Daily 2am UTC | Streaming pipeline (Task 1) → Dedup streaming sinks (Task 2) → Batch companion (Task 3) |
| **System Tables - One-Time Setup** | Manual | Creates target catalog and 12 schemas with predictive optimization enabled |
| **System Tables - Check Freshness** | Daily 8am UTC | Alerts if archive >48h stale (5-day buffer before 168h VACUUM window) |

## Design Principles

### 1. Never Delete Archive Data

Sinks are append-only. Full Refresh re-appends -- it never drops or truncates the target table. This is the fundamental safety guarantee of the archive.

### 2. Streaming First, Batch as Fallback

Streaming via SDP Delta sinks is the preferred ingestion method because it provides exactly-once append semantics with checkpoint-based recovery. Batch is used only when streaming is unavailable (see [ingestion-strategy.md](ingestion-strategy.md)).

### 3. Minimize Daily Compute Cost

- **Streaming tables**: Read only new data since last checkpoint (incremental by design).
- **Dedup step**: Checks for duplicates first; only rewrites tables that actually have dupes (scan-only on clean tables).
- **Watermark MERGE tables**: Read only data newer than `max(watermark) - buffer` from the archive.
- **Full overwrite tables**: Reserved for small reference tables (hundreds to low thousands of rows).
- **Table optimizations**: Automatic liquid clustering (`CLUSTER BY AUTO`) and predictive optimization reduce scan costs over time.

### 4. Fail Safe, Alert Fast

- Each streaming flow is independent; one failure doesn't block others.
- Batch tables are wrapped in try/except; failures are logged but don't stop the run.
- Freshness check alerts at 48 hours, providing 5 days of buffer before the 168-hour VACUUM window is breached.

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Streaming pipeline | SDP (Spark Declarative Pipelines) | Incremental streaming with Delta sinks |
| Dedup notebook | Databricks Notebook (Serverless) | Post-pipeline duplicate removal with skip optimization |
| Batch notebook | Databricks Notebook (Serverless) | Watermark MERGE + full overwrite |
| Packaging | Databricks Asset Bundles | Multi-environment deployment |
| Compute | Serverless | No cluster management overhead |
| Storage | Delta Lake (Unity Catalog) | ACID transactions, schema evolution |
| Table optimization | Liquid Clustering + Predictive Optimization | Auto-tuned layout and maintenance |
| Monitoring | SQL-based freshness check | Staleness alerting |

## Data Flow

| Step | Component | Action |
|------|-----------|--------|
| 1 | Workflow scheduler | Triggers daily at 2am UTC |
| 2 | SDP Pipeline (Task 1) | Reads incrementally from 27 system tables via `readStream` with `skipChangeCommits=true`. 4 tables with DeletionVectors use `responseFormat=delta`. |
| 3 | Delta Sinks | Appends new rows to archive tables in `${target_catalog}` |
| 4 | Dedup Notebook (Task 2) | Runs after pipeline completes (`run_if: ALL_DONE`) |
| 5 | Duplicate check | For each of 27 streaming tables, checks for duplicate key groups via `GROUP BY ... HAVING COUNT(*) > 1 LIMIT 1`. If clean, skips the table (no rewrite). |
| 6 | Dedup rewrite | If duplicates found, uses `INSERT OVERWRITE` with `ROW_NUMBER()` to atomically remove them. Also ensures `CLUSTER BY AUTO` on all tables. |
| 7 | Batch Notebook (Task 3) | Runs after dedup completes (`run_if: ALL_DONE`) |
| 8 | Watermark MERGE | Reads rows newer than `max(watermark) - buffer` from 4 source tables, MERGEs into archive |
| 9 | Full Overwrite | Overwrites 6 small reference tables in archive |
| 10 | Freshness Check (separate job) | Queries `information_schema.tables` at 8am UTC, raises error if any table >48h stale |

## Integration Points

| Integration | Direction | Protocol | Notes |
|-------------|-----------|----------|-------|
| Delta Sharing (system tables) | Inbound | Delta Sharing | Source tables delivered via sharing; requires `skipChangeCommits=true`. Some tables require `responseFormat=delta` for DeletionVectors compatibility. |
| Unity Catalog | Bidirectional | UC API | Archive tables are UC-managed Delta tables |
| Email notifications | Outbound | SMTP | Failure alerts to workspace user |
| SQL Warehouse | Query | SQL | Freshness check runs on shared SQL warehouse |
