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
3. **Trade-off**: You will get duplicates for the overlap period. Use dedup views to handle this.

### Prevention

- Pipeline runs daily at 2am UTC.
- Freshness check job runs daily at 8am UTC and raises an error if any archive table is >48 hours stale — providing 5 days of buffer before the 168-hour VACUUM window.
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

## Duplicate Handling

Duplicates can occur in two scenarios:

### 1. After Full Refresh (Streaming Tables)

A Full Refresh re-reads all available data and appends it to the sink. Data already in the archive gets appended again.

**Mitigation**: Create dedup views for downstream consumers:

```sql
CREATE OR REPLACE VIEW ${catalog}.billing.usage_deduped AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY usage_record_id
        ORDER BY usage_date DESC
    ) AS _rn
    FROM ${catalog}.billing.usage
) WHERE _rn = 1;
```

### 2. Overlapping Watermark Windows (Batch MERGE Tables)

The 4-hour lookback buffer means some rows are re-read on consecutive runs. The MERGE with `WHEN NOT MATCHED THEN INSERT *` prevents duplicates as long as natural keys are correct.

**If natural keys are incorrect**: Duplicates may appear. Use dedup views as a safety net and correct the keys.

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
| Streaming (SDP sinks) | New columns flow automatically. No action needed. |
| Batch overwrite | `overwriteSchema=true` handles new columns automatically. |
| Batch watermark MERGE | **Manual action required.** New columns need `ALTER TABLE ADD COLUMN` on the target before the MERGE picks them up. |

## Cost Optimization

### Current Design Choices

| Choice | Cost Impact |
|--------|------------|
| Serverless compute | No idle cluster costs; pay per query |
| Streaming (incremental) | Reads only new data since checkpoint |
| Watermark MERGE (incremental) | Reads only data newer than `max(watermark) - buffer` |
| Full overwrite limited to reference tables | Only 6 tiny tables scanned fully |

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
