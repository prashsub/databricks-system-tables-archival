# Changelog

All notable changes to the System Tables Archival project.

## [1.4.0] - 2026-02-15

### Added

- Post-pipeline deduplication notebook (`src/dedup/dedup_streaming_tables.py`) that removes duplicate rows from all 27 streaming sink tables after a Full Refresh.
- Natural key registry for all 27 streaming tables, organized by category:
  - 10 event tables with unique row ID (`event_id`, `record_id`, `databricks_request_id`, etc.)
  - 2 snapshot/`_latest` tables (`experiments_latest`, `runs_latest`) keyed by `workspace_id` + entity ID
  - 6 SCD tables keyed by entity ID + `change_time`
  - 3 timeline tables keyed by run/update ID + `period_start_time` (hourly slicing)
  - 3 event tables without unique ID using composite keys (`warehouse_events`, marketplace tables)
  - 1 node timeline table keyed by `cluster_id, instance_id, start_time`
  - 2 zerobus internal tables (`stream_id` / `stream_id, commit_version`)
- Dedup task added to archival workflow between streaming pipeline and batch companion (`run_if: ALL_DONE`).
- Uses `INSERT OVERWRITE` with `ROW_NUMBER()` window function for atomic, safe dedup.
- Enabled **automatic liquid clustering** (`CLUSTER BY AUTO`) on all 37 archive tables. Dedup notebook enforces clustering on every run for new tables.
- Enabled **predictive optimization** on all 12 archive schemas. Setup notebook now enables it during initial schema creation.

### Performance

- Steady-state dedup runtime: ~15 min (24 tables, minimal/no duplicates). First run with 46B duplicates: ~79 min.

## [1.3.2] - 2026-02-13

### Fixed

- Batch companion now uses `run_if: ALL_DONE` so it runs even when the streaming pipeline fails partially. Previously, any streaming flow failure blocked all 10 batch tables.
- Documented `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` checkpoint mismatch as a known failure mode with Full Refresh recovery.

## [1.3.1] - 2026-02-12

### Fixed

- Freshness check SQL: `${target_catalog}` → `IDENTIFIER(:target_catalog || '...')` for correct SQL task parameter substitution.
- Freshness check SQL: `last_modified` → `last_altered` (correct column in `information_schema.tables`).
- Cleaned up stale schemas (`ai`, `ingest`, `ml`, `network`, `quality`, `warehouse`, `default`) from archive catalog — artifacts from earlier pipeline iterations.

## [1.3.0] - 2026-02-11

### Added

- Configurable `exclude_tables` bundle variable: comma-separated list of system tables to skip from ingestion.
- Both streaming pipeline and batch notebook parse the exclusion list and skip matching tables.
- Excluding a table stops refreshing but retains existing archive data (never drops or deletes).
- Re-including a table resumes ingestion; batch tables create schemas inline (idempotent).
- Accepts both `system.schema.table` and `schema.table` formats (auto-prefixed).
- Documentation: new "Excluding and Re-Including Tables" section in operational considerations.

## [1.2.0] - 2026-02-11

### Fixed

- Streaming: Added `responseFormat=delta` for 4 tables with upstream DeletionVectors (`inbound_network`, `pipeline_update_timeline`, `zerobus_stream`, `zerobus_ingest`). Resolves `DS_UNSUPPORTED_DELTA_TABLE_FEATURES` error.
- Batch: Fixed `data_classification.results` — watermark column corrected from `inference_run_timestamp` to `latest_detected_time`, natural keys corrected to `catalog_name, schema_name, table_name, column_name, class_tag`.
- Batch: Fixed `assistant_events` — natural key corrected from `event_time, workspace_id, conversation_id` to `event_id`.
- Batch: Moved `billing.cloud_infra_cost` from streaming to batch overwrite (not streaming-capable per docs).
- Batch: Exit value now includes error messages for failed tables (not just table names).

### Changed

- Freshness alert threshold reduced from 168h to 48h, providing 5 days of buffer before the VACUUM window.
- Separated setup into its own job (`System Tables - One-Time Setup`) — no longer runs on every scheduled ingestion.
- Removed deprecated `environments.spec.client` from all job configs; replaced with `environment_version: "4"`.

### Added

- `resources/setup_job.yml` — standalone one-time setup job.
- `.gitignore` — excludes `.claude/`, `.cursor/skills/`, `ai-dev-kit/`.

## [1.1.0] - 2026-02-10

### Changed

- Restored 5 tables to streaming (per Databricks docs) that were previously in batch due to Delta Sharing errors:
  - `access.inbound_network`, `access.outbound_network`
  - `lakeflow.pipeline_update_timeline`, `lakeflow.zerobus_stream`, `lakeflow.zerobus_ingest`
- Full overwrite now limited to 5 small reference tables only
- Applied `naming-tagging-standards` skill to all resource names and tags:
  - Job names follow `[${bundle.target}] {Domain} - {Action} {Entity}` convention
  - Pipeline names follow `[${bundle.target}] {Layer} {Domain} Pipeline` convention
  - All jobs tagged with `team`, `cost_center`, `environment`, `project`, `job_type`
- Reorganized repo structure: flattened from nested `system_tables_archival/` to `SystemTableIngestion/` root

### Added

- `docs/architecture/` documentation set explaining design decisions
- `CHANGELOG.md` for version tracking

## [1.0.0] - 2026-02-09

### Added

- Initial deployment of System Tables Archival pipeline
- SDP streaming pipeline with 23 Delta sink flows for streaming-capable tables
- Batch companion notebook with watermark MERGE (4 tables) and full overwrite (5 reference tables)
- 5 additional tables handled via batch watermark MERGE as streaming fallback
- Freshness check job alerting on >7-day staleness (Delta Sharing VACUUM window)
- One-time setup notebook creating catalog and 12 schemas
- Databricks Asset Bundle packaging with dev/prod targets
- Daily 2am UTC schedule with email notifications on failure

### Verified

- Audit confirmed 9.29 billion rows archived across 37 tables with ~100% row count match to source
- All table names verified against `system.information_schema.tables` and official Databricks docs
