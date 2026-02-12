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
|  |  +---------------------------+  +-------------------------------+ |  |
|  |  | Task 1: SDP Pipeline      |  | Task 2: Batch Companion       | |  |
|  |  |                           |  |                               | |  |
|  |  | 27 streaming tables       |  | 4 watermark MERGE tables      | |  |
|  |  | append_flow + Delta sinks |  | 6 full overwrite tables       | |  |
|  |  | Serverless, triggered     |  | Serverless compute            | |  |
|  |  +---------------------------+  +-------------------------------+ |  |
|  +-------------------------------------------------------------------+  |
|           |                                                            |
|  +--------v---------------------------------------------------------+  |
|  | ${target_catalog} (Persistent Archive)                            |  |
|  |                                                                   |  |
|  | 12 schemas, 37 tables                                             |  |
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
| **System Tables - Ingest Archive** | Daily 2am UTC | Streaming pipeline (Task 1) then batch companion (Task 2) |
| **System Tables - One-Time Setup** | Manual | Creates target catalog and 12 schemas |
| **System Tables - Check Freshness** | Daily 8am UTC | Alerts if archive >48h stale (5-day buffer before 168h VACUUM window) |

## Design Principles

### 1. Never Delete Archive Data

Sinks are append-only. Full Refresh re-appends -- it never drops or truncates the target table. This is the fundamental safety guarantee of the archive.

### 2. Streaming First, Batch as Fallback

Streaming via SDP Delta sinks is the preferred ingestion method because it provides exactly-once append semantics with checkpoint-based recovery. Batch is used only when streaming is unavailable (see [ingestion-strategy.md](ingestion-strategy.md)).

### 3. Minimize Daily Compute Cost

- **Streaming tables**: Read only new data since last checkpoint (incremental by design).
- **Watermark MERGE tables**: Read only data newer than `max(watermark) - buffer` from the archive.
- **Full overwrite tables**: Reserved for small reference tables (hundreds to low thousands of rows).

### 4. Fail Safe, Alert Fast

- Each streaming flow is independent; one failure doesn't block others.
- Batch tables are wrapped in try/except; failures are logged but don't stop the run.
- Freshness check alerts at 48 hours, providing 5 days of buffer before the 168-hour VACUUM window is breached.

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Streaming pipeline | SDP (Spark Declarative Pipelines) | Incremental streaming with Delta sinks |
| Batch notebook | Databricks Notebook (Serverless) | Watermark MERGE + full overwrite |
| Packaging | Databricks Asset Bundles | Multi-environment deployment |
| Compute | Serverless | No cluster management overhead |
| Storage | Delta Lake (Unity Catalog) | ACID transactions, schema evolution |
| Monitoring | SQL-based freshness check | Staleness alerting |

## Data Flow

| Step | Component | Action |
|------|-----------|--------|
| 1 | Workflow scheduler | Triggers daily at 2am UTC |
| 2 | SDP Pipeline (Task 1) | Reads incrementally from 27 system tables via `readStream` with `skipChangeCommits=true`. 4 tables with DeletionVectors use `responseFormat=delta`. |
| 3 | Delta Sinks | Appends new rows to archive tables in `${target_catalog}` |
| 4 | Batch Notebook (Task 2) | Runs after pipeline completes |
| 5 | Watermark MERGE | Reads rows newer than `max(watermark) - buffer` from 4 source tables, MERGEs into archive |
| 6 | Full Overwrite | Overwrites 6 small reference tables in archive |
| 7 | Freshness Check (separate job) | Queries `information_schema.tables` at 8am UTC, raises error if any table >48h stale |

## Integration Points

| Integration | Direction | Protocol | Notes |
|-------------|-----------|----------|-------|
| Delta Sharing (system tables) | Inbound | Delta Sharing | Source tables delivered via sharing; requires `skipChangeCommits=true`. Some tables require `responseFormat=delta` for DeletionVectors compatibility. |
| Unity Catalog | Bidirectional | UC API | Archive tables are UC-managed Delta tables |
| Email notifications | Outbound | SMTP | Failure alerts to workspace user |
| SQL Warehouse | Query | SQL | Freshness check runs on shared SQL warehouse |
