# Operational Considerations

## The 7-Day VACUUM Window

The single most critical operational constraint of this system.

Databricks System Tables are delivered via **Delta Sharing**. The sharing provider runs `VACUUM` on the source tables with a **7-day retention**. This means:

- Data files older than 7 days are permanently deleted from the source.
- Streaming checkpoints reference specific data file versions.
- If the streaming pipeline falls >7 days behind, checkpoints reference deleted files and become **unrecoverable**.

### Recovery When Checkpoint is Stale

1. Run a **Full Refresh** of the SDP pipeline.
2. This is **safe** -- Full Refresh re-appends all currently available data to the sinks. It never drops or truncates existing archive data.
3. **Trade-off**: You will get duplicates for the overlap period. The `dedup_streaming_sinks` task runs automatically after the pipeline and removes them.

### Prevention

- Pipeline runs daily at 2am UTC.
- Freshness check job runs daily at 8am UTC (serverless notebook `src/monitoring/freshness_check.py`) and fails if any incrementally archived table is >48 hours behind its source — providing 5 days of buffer before the 168-hour VACUUM window. For each of the 31 incremental tables, it takes the archive's latest timestamp and probes the source system table for rows newer than that plus the threshold. Comparing against the source means quiet tables with no new rows don't raise false alarms. The 6 full-overwrite tables are not checked.
  - Why not `last_altered`? The dedup task runs `ALTER TABLE ... CLUSTER BY AUTO` on every sink daily, which bumps `information_schema.tables.last_altered` even when no data arrived. A `last_altered`-based check can never fire.
- Email notifications fire on any task failure.

## `skipChangeCommits` Explained

All `readStream` calls use `.option("skipChangeCommits", "true")`. This is required because:

- Delta Sharing source tables undergo rolling deletes (old data is removed as the retention window moves forward).
- Without `skipChangeCommits`, Spark Structured Streaming treats these deletes as change events and fails.
- With `skipChangeCommits`, the stream ignores delete operations and only processes appends -- which is exactly what we want for archival.

## `responseFormat=delta` for DeletionVectors

4 system tables have `delta.enableDeletionVectors` enabled upstream. Without the `responseFormat=delta` option, Delta Sharing returns a `DS_UNSUPPORTED_DELTA_TABLE_FEATURES` error during `readStream`.

The streaming archive selectively applies this option only to the tables that need it (flagged with `"delta_format": True` in the config) to minimize memory overhead.

Affected tables: `inbound_network`, `pipeline_update_timeline`, `zerobus_stream`, `zerobus_ingest`.

## Checkpoint Corruption After Failed Runs

If the streaming pipeline fails mid-run (e.g., driver crash, OOM), some flow checkpoints may become corrupted. Symptoms include errors like:

> Delta sharing table null doesn't exist. Please delete your streaming query checkpoint and restart.

**Fix**: Run a **Full Refresh** of the pipeline to clear all checkpoints and re-read from the source.

## Checkpoint Table ID Mismatch

If the upstream Delta Sharing provider recreates or modifies a system table (e.g., during a Databricks platform upgrade), the internal Delta table ID changes. Streaming checkpoints reference the old ID and refuse to read from the new one.

**Symptom**: `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` error on one or more flows.

**Impact**: Affected flows fail. SDP treats any flow failure as a pipeline failure, but unaffected flows may still complete. The batch companion task uses `run_if: ALL_DONE` so it runs regardless of streaming outcome.

**Fix**: Run a **Full Refresh** to clear all checkpoints: `databricks pipelines start-update <pipeline-id> --full-refresh`.

**Prevention**: None — this is an upstream infrastructure event outside our control. The freshness job at 48h provides early warning before the 168h VACUUM window.

## Schema Mismatch on a Sink (`DELTA_METADATA_MISMATCH`)

**Symptom**: One flow fails with `[DELTA_METADATA_MISMATCH.SCHEMA_MISMATCH]` (sometimes with an `ACL_ENABLED` sub-error suggesting `ALTER TABLE`). Example: `lakeflow_jobs_flow` failed when Databricks added a top-level `triggers` column and a nested `trigger.paused` field to `system.lakeflow.jobs`.

**Cause**: Databricks adds columns and struct fields to system tables without notice. A Delta sink with no schema-evolution option refuses to write a wider schema to its target table.

**Why `ACL_ENABLED` appears**: On compute that enforces access controls, such as serverless, the session-wide `spark.databricks.delta.schema.autoMerge.enabled` setting isn't honored. When no per-write option is set, the error points you to `ALTER TABLE`. Setting `mergeSchema` on the sink is the per-write option, and it works on serverless (validated on Databricks Runtime 18.3).

**Fix (since 1.5.0)**: Every sink sets `"mergeSchema": "true"` in its `create_sink` options, so new columns and nested fields are added automatically. A flow that has already failed recovers on the next normal pipeline update after deploying 1.5.0:

- No full refresh is needed. Sink and flow names are unchanged, so each flow resumes from its checkpoint.
- No duplicates are written, and existing archive data is untouched.
- Don't set `autoMerge` session-wide as a workaround.

**Manual fallback**: If a change can't be applied automatically, add the missing fields by hand, then rerun the pipeline. Nested fields use dotted paths (`col.field`, `col.element.field` for array elements, `col.value.field` for map values). This needs `MODIFY` (or ownership) on the table plus `USE CATALOG` and `USE SCHEMA`.

```sql
ALTER TABLE <target_catalog>.lakeflow.jobs ADD COLUMNS (
  trigger.paused BOOLEAN,
  triggers ARRAY<STRUCT<...>>   -- copy the type from DESCRIBE system.lakeflow.jobs
);
```

Never drop and recreate the archive table to work around a schema mismatch — it holds history that no longer exists in the source.

## Duplicate Handling

Duplicates can occur in two scenarios:

### 1. After Full Refresh (Streaming Tables)

A Full Refresh re-reads all available data and appends it to the sink. Data already in the archive gets appended again.

**Mitigation**: The `dedup_streaming_sinks` workflow task runs automatically after every streaming pipeline execution. It removes duplicates from all 27 streaming sink tables using `INSERT OVERWRITE` with `ROW_NUMBER() OVER (PARTITION BY <natural_keys> ORDER BY <tiebreaker> DESC)`.

The dedup notebook (`src/dedup/dedup_streaming_tables.py`) contains the full natural key registry for all 27 tables. Each table's keys are categorized by pattern:

| Category | Pattern | Example |
|----------|---------|---------|
| Event tables with unique ID | `event_id` or `record_id` | `access.audit` → `event_id` |
| Snapshot/`_latest` tables | `workspace_id` + entity ID | `mlflow.runs_latest` → `workspace_id, run_id` |
| SCD tables | entity ID + `change_time` | `compute.clusters` → `workspace_id, cluster_id, change_time` |
| Timeline tables (hourly-sliced) | run ID + `period_start_time` | `lakeflow.job_run_timeline` → `workspace_id, run_id, period_start_time` |
| Composite key (no unique ID) | Multiple event attributes | `compute.warehouse_events` → `workspace_id, warehouse_id, event_type, event_time` |

**Safety**: `INSERT OVERWRITE` is atomic — either the full dedup succeeds or the table remains unchanged. If a table doesn't exist or is empty, it's safely skipped.

**Skip optimization**: The dedup checks for duplicate key groups first using `GROUP BY ... HAVING COUNT(*) > 1 LIMIT 1`. If a table is clean (no duplicates), the expensive `INSERT OVERWRITE` is skipped entirely. This reduces steady-state runtime from ~15 min (full rewrite of all tables) to ~5 min (scan-only).

| Scenario | Dedup Runtime | Action Taken |
|----------|--------------|--------------|
| After Full Refresh (billions of dupes) | ~79 min | Full `INSERT OVERWRITE` on all tables |
| Steady state (no/few dupes) | ~5 min | Scan-only; skip rewrite on clean tables |

### 1b. Source-Side Compaction Duplicates (skipChangeCommits)

Even on normal incremental runs, a small number of duplicates (~1K) can appear. This is caused by `skipChangeCommits=true` behavior when Databricks runs OPTIMIZE/VACUUM on the source system tables:

1. Source table files are compacted (rows deleted from old files, re-inserted into new files)
2. `skipChangeCommits` correctly ignores the delete operations
3. But the re-inserts from compaction appear as "new" data to the streaming reader
4. These rows get re-appended to the sink, creating duplicates

The dedup task catches and removes these automatically. The cost is negligible (~1K rows out of billions).

### 2. Overlapping Watermark Windows (Batch MERGE Tables)

The 4-hour lookback buffer means some rows are re-read on consecutive runs. The MERGE with `WHEN NOT MATCHED THEN INSERT *` prevents duplicates as long as natural keys are correct.

**If natural keys are incorrect**: Duplicates may appear. Fix the keys in the batch companion notebook's table config.

## Excluding and Re-Including Tables

The `exclude_tables` bundle variable lets you skip specific tables without editing code.

### Excluding a Table

Set `exclude_tables` in `databricks.yml` under the target (recommended for multi-table exclusion):

```yaml
targets:
  dev:
    variables:
      exclude_tables: "system.marketplace.listing_funnel_events,system.marketplace.listing_access_events"
```

For single-table exclusion, the CLI works too: `databricks bundle deploy --var="exclude_tables=system.marketplace.listing_funnel_events"`.

> **Note:** The `--var` CLI flag interprets commas as value separators. For multi-table exclusion, set the variable in `databricks.yml` instead of passing via `--var`.

**Behavior**: The table stops being refreshed on subsequent runs. The existing archive table and its data are **never deleted** — they remain in the catalog for querying.

### Re-Including a Previously Excluded Table

Remove the table from `exclude_tables` and redeploy.

- **Streaming tables**: A Full Refresh may be needed if the checkpoint for the table is stale or missing. If the table's schema doesn't exist yet, run the setup job first (it's idempotent).
- **Batch watermark tables**: The watermark picks up from where it left off. The batch notebook creates schemas inline (`CREATE SCHEMA IF NOT EXISTS`), so no manual setup is needed.
- **Batch overwrite tables**: Resume immediately with the next full overwrite. Schema is created inline.

### Per-Target Exclusion

```yaml
targets:
  dev:
    variables:
      exclude_tables: "system.marketplace.listing_funnel_events,system.marketplace.listing_access_events"
  prod:
    variables:
      exclude_tables: ""
```

## Adding a New System Table

When Databricks releases a new system table:

1. Check the [system tables documentation](https://docs.databricks.com/aws/en/admin/system-tables/) for streaming support.
2. **If streaming-capable**: Add to `STREAMING_TABLES` in `src/streaming_etl/transformations/streaming_archive.py`. If the table has DeletionVectors enabled, set `"delta_format": True`.
3. **If batch-only**: Determine if it's a growing event table (use watermark MERGE) or a small reference table (use overwrite). Add to the appropriate list in `src/batch/batch_companion.py`.
4. If the table uses a **new schema**, add the schema to `src/setup/00_setup.py` and run the setup job. (Schemas are also created inline by the streaming and batch code, but adding to setup ensures consistency.)
5. Redeploy: `databricks bundle deploy`.
6. Run a Full Refresh (streaming) or the batch notebook to backfill historical data.

## Schema Evolution

| Strategy | Schema Evolution Behavior |
|----------|--------------------------|
| Streaming (SDP sinks) | Sinks set `mergeSchema=true`, so new top-level columns and nested struct, array-element, and map-value fields are added automatically. Without it, the flow fails with `DELTA_METADATA_MISMATCH` — see above. |
| Batch overwrite | `overwriteSchema=true` handles new columns automatically. |
| Batch watermark MERGE | `MERGE WITH SCHEMA EVOLUTION` adds new top-level columns and nested fields automatically. A plain `MERGE INTO ... INSERT *` silently drops new top-level columns and fails with `DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION` on new nested fields. |

All schema evolution is additive. Existing columns are never changed or dropped, so archived history is untouched. Non-additive upstream changes, such as a column type change, are not evolved automatically and will fail the write. Investigate those case by case. For missing columns that weren't added automatically, use the manual `ALTER TABLE ... ADD COLUMNS` fallback described in the `DELTA_METADATA_MISMATCH` section above.

## Cost Optimization

### Current Design Choices

| Choice | Cost Impact |
|--------|------------|
| Serverless compute | No idle cluster costs; pay per query |
| Streaming (incremental) | Reads only new data since checkpoint |
| Dedup with skip optimization | Scan-only on clean tables (~5 min); rewrites only when duplicates exist |
| Watermark MERGE (incremental) | Reads only data newer than `max(watermark) - buffer` |
| Full overwrite limited to reference tables | Only 6 tiny tables scanned fully |
| CLUSTER BY AUTO | Automatic liquid clustering reduces scan cost for downstream queries |
| Predictive Optimization | Auto OPTIMIZE/VACUUM/ZORDER reduces storage and improves query performance |

### Typical Daily Runtime

| Task | Steady State | After Full Refresh |
|------|-------------|-------------------|
| Streaming pipeline | ~2 min | ~13 min |
| Dedup streaming sinks | ~5 min (scan-only) | ~79 min (full rewrite) |
| Batch companion | ~4 min | ~5 min |
| **Total** | **~11 min** | **~97 min** |

### Monitoring Cost

Check the job's DBU consumption in `system.billing.usage`:

```sql
SELECT
    usage_date,
    sku_name,
    SUM(usage_quantity) AS total_dbus
FROM system.billing.usage
WHERE usage_metadata.job_id = '<archival_job_id>'
GROUP BY usage_date, sku_name
ORDER BY usage_date DESC
```
