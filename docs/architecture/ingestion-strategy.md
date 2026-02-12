# Ingestion Strategy: Streaming vs. Batch

## Decision Framework

Every Databricks system table is assigned to one of three ingestion strategies based on a decision tree:

```
Is the table streaming-capable (per Databricks docs)?
  |
  +-- YES --> STRATEGY A: SDP Streaming + Delta Sink (27 tables)
  |             Note: Tables with DeletionVectors need responseFormat=delta
  |
  +-- NO  --> Is the table an event/timeline table that grows over time?
                |
                +-- YES --> Does the table have a timestamp watermark?
                |             |
                |             +-- YES --> STRATEGY B: Batch Watermark MERGE (4 tables)
                |             +-- NO  --> STRATEGY C: Batch Full Overwrite
                |
                +-- NO  --> STRATEGY C: Batch Full Overwrite (6 tables)
```

## Strategy A: SDP Streaming + Delta Sink

**When to use**: Table supports `readStream` via Delta Sharing.

**How it works**:
1. SDP pipeline registers a **Delta sink** per table -- a persistent archive table outside the pipeline lifecycle.
2. An `append_flow` reads incrementally from the source with `skipChangeCommits=true`.
3. Tables with upstream DeletionVectors additionally set `responseFormat=delta` (see Delta Format section below).
4. New rows are appended to the sink. Existing data is never modified or deleted.
5. Checkpoints track exactly where the stream left off.

**Why this is preferred**:
- **Exactly-once semantics**: Checkpoint-based recovery prevents duplicates under normal operation.
- **Minimal compute**: Only reads new data since last checkpoint.
- **No key/watermark guessing**: The stream handles incremental logic automatically.

**Risk**: If the pipeline falls >7 days behind, Delta Sharing VACUUMs the source and checkpoints become unrecoverable. Recovery requires a Full Refresh (which is safe -- it re-appends, never deletes).

### Delta Format for DeletionVectors

4 tables have `delta.enableDeletionVectors` enabled upstream. Without `responseFormat=delta`, Delta Sharing returns a `DS_UNSUPPORTED_DELTA_TABLE_FEATURES` error. These tables are flagged with `"delta_format": True` in the streaming config:

- `system.access.inbound_network`
- `system.lakeflow.pipeline_update_timeline`
- `system.lakeflow.zerobus_stream`
- `system.lakeflow.zerobus_ingest`

**Tables (27)**:
| Schema | Tables |
|--------|--------|
| access | audit, column_lineage, table_lineage, clean_room_events, inbound_network*, outbound_network |
| billing | usage |
| compute | clusters, node_timeline, warehouse_events, warehouses |
| lakeflow | job_run_timeline, job_task_run_timeline, job_tasks, jobs, pipelines, pipeline_update_timeline*, zerobus_stream*, zerobus_ingest* |
| marketplace | listing_funnel_events, listing_access_events |
| mlflow | experiments_latest, runs_latest, run_metrics_history |
| serving | served_entities, endpoint_usage |
| sharing | materialization_history |

`*` = requires `responseFormat=delta`

## Strategy B: Batch Watermark MERGE

**When to use**: Table is not streamable AND the table has a reliable timestamp column that increases monotonically.

**How it works**:
1. Query the archive for `max(watermark_column)`.
2. Read source rows where `watermark_column >= max - buffer` (default 4-hour buffer for late-arriving data).
3. MERGE into archive with `WHEN NOT MATCHED THEN INSERT *` using null-safe `<=>` joins on natural keys.
4. On first run, performs a full load.

**Why watermark MERGE instead of full overwrite**:
- Event/timeline tables can have **millions to billions of rows**.
- Full overwrite would scan the entire source table every day -- expensive and slow.
- Watermark MERGE reads only the incremental delta (typically hours or days of data).

**Trade-off**: Requires knowing the correct watermark column and natural keys. Incorrect keys can cause duplicates (too narrow) or missed rows (too broad). Dedup views provide a safety net.

### Non-Streaming Tables (4)

These tables are documented as non-streaming by Databricks:

| Source | Watermark | Natural Keys | Notes |
|--------|-----------|-------------|-------|
| `system.query.history` | `end_time` | `statement_id` | Largest single table (~billions of rows) |
| `system.data_classification.results` | `latest_detected_time` | `catalog_name`, `schema_name`, `table_name`, `column_name`, `class_tag` | Composite key |
| `system.access.assistant_events` | `event_time` | `event_id` | Docs list as non-streaming |
| `system.storage.predictive_optimization_operations_history` | `start_time` | `operation_id` | Docs list as non-streaming |

## Strategy C: Batch Full Overwrite

**When to use**: Table is small (hundreds to low thousands of rows), slowly changing, and does not have a meaningful timestamp for incremental reads.

**How it works**:
1. Read the entire source table.
2. Overwrite the archive table with `mode("overwrite")` and `overwriteSchema=true`.

**Why this is acceptable**:
- Reference tables are tiny -- the full scan costs fractions of a cent.
- Schema changes are handled automatically via `overwriteSchema`.
- No watermark or key logic needed -- simplest possible approach.

**Tables (6)**:
| Source | Approximate Size | Nature |
|--------|-----------------|--------|
| `system.billing.list_prices` | ~hundreds of rows | SKU price list |
| `system.billing.account_prices` | ~hundreds of rows | Account-specific pricing |
| `system.billing.cloud_infra_cost` | ~hundreds of rows | Cloud infrastructure cost data (not streaming-capable) |
| `system.compute.node_types` | ~hundreds of rows | VM type enumeration |
| `system.access.workspaces_latest` | ~tens of rows | Workspace metadata snapshot |
| `system.data_quality_monitoring.table_results` | ~thousands of rows | DQ monitoring results |

## Why Not Use Streaming for Everything?

1. **Some tables don't support streaming** -- Databricks explicitly documents certain tables as batch-only (e.g., `query.history`).
2. **Reference tables don't benefit from streaming** -- For a table with 200 rows that changes monthly, the overhead of maintaining a streaming checkpoint exceeds the cost of a daily full overwrite.

## Why Not Use Batch for Everything?

1. **Cost**: A full table scan of `system.access.audit` (billions of rows) every day would be extremely expensive.
2. **Complexity**: Watermark MERGE requires knowing the correct timestamp column and natural keys. Streaming handles this automatically via checkpoints.
3. **Correctness**: Streaming provides exactly-once append semantics. Watermark MERGE requires careful key selection to avoid duplicates.

## Summary

| Strategy | Tables | Cost Profile | Complexity | Correctness |
|----------|--------|-------------|------------|-------------|
| **A: SDP Streaming** | 27 | Minimal (incremental) | Low (automatic) | High (exactly-once) |
| **B: Watermark MERGE** | 4 | Low (incremental) | Medium (keys required) | Good (dedup views as safety net) |
| **C: Full Overwrite** | 6 | Negligible (tiny tables) | Lowest | Perfect (full replace) |
| **Total** | **37** | | | |
