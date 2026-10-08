# System Tables Incremental Archival — Databricks Asset Bundle

Production solution for archiving all Databricks System Tables into persistent Delta tables, preserving data indefinitely beyond the system tables' rolling retention windows (30-365 days).

Packaged as a **Databricks Asset Bundle (DAB)** with multi-environment support (dev/prod).

## Architecture

```
+---------------------------------------------------------------------------------+
|  Databricks Workflow: System Tables - Ingest Archive (daily 2am)                |
|                                                                                 |
|  +--------------------+  +--------------------+  +--------------------+         |
|  | Task 1: SDP        |  | Task 2: Dedup      |  | Task 3: Batch     |         |
|  | Pipeline           |->| Streaming Sinks    |->| Companion         |         |
|  |                    |  |                    |  |                    |         |
|  | 27 streaming       |  | Skip if clean      |  | 4 watermark MERGE  |         |
|  | tables via         |  | INSERT OVERWRITE   |  | 6 full overwrite   |         |
|  | append_flow +      |  | if dupes found     |  | Serverless         |         |
|  | Delta sinks        |  | + CLUSTER BY AUTO  |  |                    |         |
|  +--------+-----------+  +--------+-----------+  +--------+-----------+         |
|           |                       |                        |                    |
|           v                       v                        v                    |
|  +---------------------------------------------------------------------+       |
|  |      ${var.target_catalog} (Unity Catalog)                           |       |
|  |                                                                      |       |
|  |  12 schemas, 37 tables                                               |       |
|  |  CLUSTER BY AUTO + Predictive Optimization enabled                   |       |
|  +---------------------------------------------------------------------+       |
+---------------------------------------------------------------------------------+
```

For detailed architecture, design principles, and data flow, see [Architecture Overview](docs/architecture/architecture-overview.md).

## Project Structure

```
system-tables-archival/
+-- databricks.yml                              # Bundle config + targets (dev/prod)
+-- resources/
|   +-- streaming_pipeline.yml                  # SDP pipeline resource definition
|   +-- archival_workflow.yml                   # Scheduled workflow (streaming + dedup + batch)
|   +-- setup_job.yml                           # One-time setup job (catalog/schema creation)
|   +-- freshness_alert.yml                     # Freshness job: fails if any table lags its source > 48 hours
+-- src/
|   +-- common/
|   |   +-- table_features.py                   # Row tracking + change data feed enablement
|   +-- setup/
|   |   +-- 00_setup.py                         # One-time catalog/schema creation + predictive optimization
|   +-- streaming_etl/
|   |   +-- transformations/
|   |       +-- streaming_archive.py            # SDP pipeline -- raw .py (not notebook)
|   +-- dedup/
|   |   +-- dedup_streaming_tables.py           # Post-pipeline dedup with skip optimization
|   |   +-- dedup_logic.py                      # Natural key registry + dedup SQL (unit tested)
|   +-- batch/
|   |   +-- batch_companion.py                  # Batch notebook -- MERGE + overwrite
|   +-- monitoring/
|       +-- freshness_check.py                  # Per-table freshness notebook (archive vs. source lag)
+-- docs/
|   +-- architecture/
|       +-- architecture-overview.md            # System architecture and design principles
|       +-- ingestion-strategy.md               # Why streaming vs. batch for each table
|       +-- operational-considerations.md       # VACUUM window, duplicates, schema evolution
+-- tests/
|   +-- unit/                                   # pytest: uv run --with pytest pytest -q
|   +-- integration/                            # Databricks notebook: dedup + row tracking on real Delta tables
+-- QUICKSTART.md                               # Commands-only quick start
+-- CHANGELOG.md                                # Version history
```

## Jobs

| Job | Schedule | Purpose |
|-----|----------|---------|
| **System Tables - Ingest Archive** | Daily 2am UTC | Streaming pipeline → Dedup streaming sinks → Batch companion |
| **System Tables - One-Time Setup** | Manual (no schedule) | Creates target catalog and schemas with predictive optimization |
| **System Tables - Check Freshness** | Daily 8am UTC | Serverless notebook. Fails and emails if any archive table lags its source system table by >48h (VACUUM window is 168h). No SQL warehouse needed. |

## Variables

| Variable | Description | Dev Default | Prod Default |
|----------|-------------|-------------|--------------|
| `target_catalog` | Unity Catalog catalog for archived tables | `system_tables_archive_dev` | `system_tables_archive` |
| `exclude_tables` | Comma-separated system tables to skip (e.g. `system.marketplace.listing_funnel_events`) | `""` (archive all) | `""` (archive all) |

## Table Assignments (37 tables)

| Strategy | Count | Description |
|----------|-------|-------------|
| **SDP Streaming** | 27 | Incremental via `append_flow` + Delta sinks. 4 tables require `responseFormat=delta` for DeletionVectors. Post-pipeline dedup removes any duplicates. |
| **Batch Watermark MERGE** | 4 | Incremental via timestamp watermark + MERGE on natural keys. |
| **Batch Full Overwrite** | 6 | Small reference tables replaced daily. |

## Table Optimizations

All 37 archive tables have the following optimizations enabled:

| Optimization | Scope | Description |
|-------------|-------|-------------|
| **CLUSTER BY AUTO** | All tables | Automatic liquid clustering — Delta selects optimal clustering columns based on query patterns |
| **Predictive Optimization** | All schemas | Databricks automatically runs OPTIMIZE, VACUUM, and ZORDER based on usage patterns |
| **Row tracking + change data feed** | 27 streaming sinks + 4 watermark MERGE tables | Stable `_metadata.row_id` per row (the dedup keeps the earliest-written copy) and row-level changes for incremental downstream reads via `table_changes()` / `readChangeFeed`. Not enabled on the 6 full-overwrite tables. |

These optimizations are enforced by the setup notebook (schema-level), the dedup notebook (streaming sinks, on every run) and the batch notebook (watermark tables). Row tracking and CDF are switched on the first time each notebook runs after upgrading to 1.6.0; see [Row Tracking and Change Data Feed](docs/architecture/operational-considerations.md#row-tracking-and-change-data-feed).

For the complete table-by-table breakdown and decision framework, see [Ingestion Strategy](docs/architecture/ingestion-strategy.md).

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for commands-only setup, or follow the detailed steps below.

### Prerequisites

- Unity Catalog enabled workspace
- System tables enabled (account-level)
- Databricks CLI >= 0.281.0 installed
- `CREATE CATALOG` / `CREATE SCHEMA` permissions for the service principal

### First-Time Deployment

1. Configure your Databricks CLI profile (authentication).
2. Update `databricks.yml` targets with your workspace host and profile.
3. Validate and deploy:
   ```bash
   databricks bundle validate
   databricks bundle deploy
   ```
4. Run the one-time setup job:
   ```bash
   databricks bundle run system_tables_setup
   ```
5. Run the full workflow to verify:
   ```bash
   databricks bundle run system_tables_archival_workflow
   ```

## Operations

For the full operational runbook including failure recovery, duplicate handling, schema evolution, cost monitoring, and adding new tables, see [Operational Considerations](docs/architecture/operational-considerations.md).

Key points:

- **VACUUM window**: System tables source data is vacuumed after 7 days. The freshness job checks each incremental table against its source and fails if the source has rows more than 48h newer than the archive, giving 5 days to remediate. Quiet tables with no new source rows don't raise false alarms.
- **Schema evolution is automatic**: Databricks adds columns and struct fields to system tables without notice. Streaming sinks use `mergeSchema` and the batch MERGE uses `MERGE WITH SCHEMA EVOLUTION`, so new fields are added to the archive. Changes are additive only.
- **Full Refresh only through the Ingest Archive job**: A full refresh re-appends everything still in the source to the sinks and never deletes archive data, but it leaves those rows duplicated until the job's `dedup_streaming_sinks` task runs. Trigger it as a full-refresh run of the **System Tables - Ingest Archive** job (see [QUICKSTART](QUICKSTART.md#troubleshooting)), never as a standalone pipeline update. Once row tracking is on, the dedup keeps the earliest-written copy of each row, so downstream edits to archived rows survive the re-append. The exception is the 3 latest-state tables (`mlflow.experiments_latest`, `mlflow.runs_latest`, `lakeflow.zerobus_stream`), where the newest version still wins. If any table can't be deduplicated or verified, the task fails and emails you.
- **Never DROP or TRUNCATE** sink target tables -- this is your long-term archive.
- **Dedup cost**: ~5 minutes on clean runs (scan-only). Only rewrites tables with actual duplicates.

## Known Limitations

1. **`Trigger.AvailableNow` on older runtimes**: Delta Sharing streaming supports `AvailableNow` on Databricks Runtime 18 and above. On older runtimes it's converted to `Trigger.Once`, which doesn't affect correctness.
2. **7-day checkpoint staleness**: If the pipeline falls >7 days behind, checkpoints become unrecoverable. Recovery: a full-refresh run of the Ingest Archive job, so the dedup task removes the re-appended rows. Data the source has already vacuumed can't be recovered.
3. **No expectations on sinks**: SDP data quality checks are not supported on Delta sinks.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture Overview](docs/architecture/architecture-overview.md) | System design, data flow, technology stack, integration points |
| [Ingestion Strategy](docs/architecture/ingestion-strategy.md) | Decision framework, per-table assignments, strategy trade-offs |
| [Operational Considerations](docs/architecture/operational-considerations.md) | VACUUM window, duplicates, schema evolution, cost, adding tables |
| [QUICKSTART.md](QUICKSTART.md) | Commands-only quick reference |
| [CHANGELOG.md](CHANGELOG.md) | Version history |

## Naming and Tagging Standards

| Resource | Name |
|----------|------|
| Archival workflow | `[${bundle.target}] System Tables - Ingest Archive` |
| SDP pipeline | `[${bundle.target}] Archive System Tables Pipeline` |
| Setup job | `[${bundle.target}] System Tables - One-Time Setup` |
| Freshness check | `[${bundle.target}] System Tables - Check Freshness` |

All jobs include required tags: `team`, `cost_center`, `environment`, `project`, `job_type`.
