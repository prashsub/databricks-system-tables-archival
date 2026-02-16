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
|   +-- freshness_alert.yml                     # Alert: archive stale > 48 hours
+-- src/
|   +-- setup/
|   |   +-- 00_setup.py                         # One-time catalog/schema creation + predictive optimization
|   +-- streaming_etl/
|   |   +-- transformations/
|   |       +-- streaming_archive.py            # SDP pipeline -- raw .py (not notebook)
|   +-- dedup/
|   |   +-- dedup_streaming_tables.py           # Post-pipeline dedup with skip optimization
|   +-- batch/
|   |   +-- batch_companion.py                  # Batch notebook -- MERGE + overwrite
|   +-- monitoring/
|       +-- freshness_check.sql                 # Freshness SQL for staleness alert
+-- docs/
|   +-- architecture/
|       +-- architecture-overview.md            # System architecture and design principles
|       +-- ingestion-strategy.md               # Why streaming vs. batch for each table
|       +-- operational-considerations.md       # VACUUM window, duplicates, schema evolution
+-- QUICKSTART.md                               # Commands-only quick start
+-- CHANGELOG.md                                # Version history
```

## Jobs

| Job | Schedule | Purpose |
|-----|----------|---------|
| **System Tables - Ingest Archive** | Daily 2am UTC | Streaming pipeline → Dedup streaming sinks → Batch companion |
| **System Tables - One-Time Setup** | Manual (no schedule) | Creates target catalog and schemas with predictive optimization |
| **System Tables - Check Freshness** | Daily 8am UTC | Alerts if archive >48h stale (VACUUM window is 168h) |

## Variables

| Variable | Description | Dev Default | Prod Default |
|----------|-------------|-------------|--------------|
| `target_catalog` | Unity Catalog catalog for archived tables | `system_tables_archive_dev` | `system_tables_archive` |
| `exclude_tables` | Comma-separated system tables to skip (e.g. `system.marketplace.listing_funnel_events`) | `""` (archive all) | `""` (archive all) |
| `warehouse_id` | SQL Warehouse for freshness checks | Looked up by name ("Shared endpoint") | Looked up by name |

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

These optimizations are enforced by the setup notebook (schema-level) and the dedup notebook (table-level, on every run).

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

- **VACUUM window**: System tables source data is vacuumed after 7 days. The freshness alert fires at 48h, giving 5 days to remediate.
- **Full Refresh is safe**: Re-appends to sinks, never deletes existing archive data. The dedup task automatically removes the resulting duplicates.
- **Never DROP or TRUNCATE** sink target tables -- this is your long-term archive.
- **Dedup cost**: ~5 minutes on clean runs (scan-only). Only rewrites tables with actual duplicates.

## Known Limitations

1. **`Trigger.AvailableNow` converted to `Trigger.Once`**: Delta Sharing streaming converts `AvailableNow` to `Once`. Expected, does not affect correctness.
2. **7-day checkpoint staleness**: If the pipeline falls >7 days behind, checkpoints become unrecoverable. Recovery: Full Refresh.
3. **No expectations on sinks**: SDP data quality checks are not supported on Delta sinks.
4. **Schema evolution for MERGE tables**: New columns require manual `ALTER TABLE ADD COLUMN` on the target.

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
