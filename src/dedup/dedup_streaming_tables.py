# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Archive — Deduplicate Streaming Sink Tables
# MAGIC
# MAGIC After a **Full Refresh** of the streaming pipeline, Delta sinks accumulate
# MAGIC duplicate rows (the pipeline re-reads the full source and appends again).
# MAGIC
# MAGIC This notebook removes duplicates from all 27 streaming sink tables using:
# MAGIC ```
# MAGIC ROW_NUMBER() OVER (PARTITION BY <natural_keys> ORDER BY <tiebreaker> DESC) = 1
# MAGIC ```
# MAGIC
# MAGIC **Safety**: Each table is overwritten atomically with `INSERT OVERWRITE`.
# MAGIC If the notebook fails mid-way, already-processed tables are clean and
# MAGIC unprocessed tables still have their (duplicated) data intact.

# COMMAND ----------

import json
import time
import traceback
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("target_catalog", "system_tables_archive", "Target Catalog")

# COMMAND ----------

TARGET_CATALOG = dbutils.widgets.get("target_catalog")
print(f"Target catalog: {TARGET_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Natural Key Registry
# MAGIC
# MAGIC Each entry defines the natural key (dedup partition columns) and tiebreaker
# MAGIC (ordering column to keep the "latest" row when duplicates exist).
# MAGIC
# MAGIC Sources: Databricks docs, `information_schema.columns`, `DESCRIBE` output.
# MAGIC See plan file for detailed evidence per table.

# COMMAND ----------

DEDUP_KEYS = {
    # -----------------------------------------------------------------------
    # Category 1: Event tables with unique row ID
    # -----------------------------------------------------------------------
    "access.audit": {
        "natural_keys": ["event_id"],
        "tiebreaker": "event_time",
    },
    "access.column_lineage": {
        "natural_keys": ["record_id"],
        "tiebreaker": "event_time",
    },
    "access.table_lineage": {
        "natural_keys": ["record_id"],
        "tiebreaker": "event_time",
    },
    "access.clean_room_events": {
        "natural_keys": ["event_id"],
        "tiebreaker": "event_time",
    },
    "access.inbound_network": {
        "natural_keys": ["event_id"],
        "tiebreaker": "event_time",
    },
    "access.outbound_network": {
        "natural_keys": ["event_id"],
        "tiebreaker": "event_time",
    },
    "billing.usage": {
        "natural_keys": ["record_id"],
        "tiebreaker": "usage_date",
    },
    "serving.endpoint_usage": {
        "natural_keys": ["databricks_request_id"],
        "tiebreaker": "request_time",
    },
    "sharing.materialization_history": {
        "natural_keys": ["sharing_materialization_id"],
        "tiebreaker": "created_at",
    },
    "mlflow.run_metrics_history": {
        "natural_keys": ["record_id"],
        "tiebreaker": "insert_time",
    },

    # -----------------------------------------------------------------------
    # Category 2: Snapshot / _latest tables (entity PK, keep latest state)
    # -----------------------------------------------------------------------
    "mlflow.experiments_latest": {
        "natural_keys": ["workspace_id", "experiment_id"],
        "tiebreaker": "update_time",
    },
    "mlflow.runs_latest": {
        "natural_keys": ["workspace_id", "run_id"],
        "tiebreaker": "update_time",
    },

    # -----------------------------------------------------------------------
    # Category 3: SCD (Slowly Changing Dimension) tables
    # -----------------------------------------------------------------------
    "compute.clusters": {
        "natural_keys": ["workspace_id", "cluster_id", "change_time"],
        "tiebreaker": "change_time",
    },
    "compute.warehouses": {
        "natural_keys": ["workspace_id", "warehouse_id", "change_time"],
        "tiebreaker": "change_time",
    },
    "lakeflow.jobs": {
        "natural_keys": ["workspace_id", "job_id", "change_time"],
        "tiebreaker": "change_time",
    },
    "lakeflow.job_tasks": {
        "natural_keys": ["workspace_id", "job_id", "task_key", "change_time"],
        "tiebreaker": "change_time",
    },
    "lakeflow.pipelines": {
        "natural_keys": ["workspace_id", "pipeline_id", "change_time"],
        "tiebreaker": "change_time",
    },
    "serving.served_entities": {
        "natural_keys": ["served_entity_id", "change_time"],
        "tiebreaker": "change_time",
    },

    # -----------------------------------------------------------------------
    # Category 4: Timeline tables (hourly-sliced runs)
    # -----------------------------------------------------------------------
    "lakeflow.job_run_timeline": {
        "natural_keys": ["workspace_id", "run_id", "period_start_time"],
        "tiebreaker": "period_start_time",
    },
    "lakeflow.job_task_run_timeline": {
        "natural_keys": ["workspace_id", "run_id", "period_start_time"],
        "tiebreaker": "period_start_time",
    },
    "lakeflow.pipeline_update_timeline": {
        "natural_keys": ["workspace_id", "update_id", "period_start_time"],
        "tiebreaker": "period_start_time",
    },

    # -----------------------------------------------------------------------
    # Category 5: Event tables WITHOUT unique ID (composite keys)
    # -----------------------------------------------------------------------
    "compute.warehouse_events": {
        "natural_keys": ["workspace_id", "warehouse_id", "event_type", "event_time"],
        "tiebreaker": "event_time",
    },
    "marketplace.listing_funnel_events": {
        "natural_keys": ["listing_id", "event_type", "event_time", "consumer_cloud", "consumer_region"],
        "tiebreaker": "event_time",
    },
    "marketplace.listing_access_events": {
        "natural_keys": ["listing_id", "event_type", "event_time", "consumer_email"],
        "tiebreaker": "event_time",
    },

    # -----------------------------------------------------------------------
    # Category 6: Node timeline (composite key from metric snapshots)
    # -----------------------------------------------------------------------
    "compute.node_timeline": {
        "natural_keys": ["cluster_id", "instance_id", "start_time"],
        "tiebreaker": "start_time",
    },

    # -----------------------------------------------------------------------
    # Category 7: Zerobus internal tables
    # -----------------------------------------------------------------------
    "lakeflow.zerobus_stream": {
        "natural_keys": ["stream_id"],
        "tiebreaker": "event_time",
    },
    "lakeflow.zerobus_ingest": {
        "natural_keys": ["stream_id", "commit_version"],
        "tiebreaker": "commit_time",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dedup Logic

# COMMAND ----------

def dedup_table(schema: str, table: str, natural_keys: list, tiebreaker: str) -> dict:
    """Remove duplicates from a single sink table using ROW_NUMBER window function.

    Optimization: checks for duplicate key groups first. If none exist, skips the
    expensive INSERT OVERWRITE entirely. This reduces steady-state runtime from
    ~15 min (full rewrite of all tables) to ~3-5 min (scan-only).

    Uses INSERT OVERWRITE for atomic replacement — either the full dedup succeeds
    or the table remains unchanged.
    """
    target_fqn = f"{TARGET_CATALOG}.{schema}.{table}"
    start_ts = time.time()

    if not spark.catalog.tableExists(target_fqn):
        elapsed = time.time() - start_ts
        print(f"  [{schema}.{table}] SKIPPED — table does not exist")
        return {
            "table": f"{schema}.{table}",
            "status": "skipped",
            "reason": "table_not_found",
            "elapsed_s": round(elapsed, 1),
        }

    # Check for duplicates before rewriting — GROUP BY + HAVING is much cheaper
    # than INSERT OVERWRITE when no duplicates exist (scan-only, no shuffle/write).
    partition_cols = ", ".join(natural_keys)
    dup_check_sql = f"""
        SELECT COUNT(*) AS dup_groups FROM (
            SELECT {partition_cols}
            FROM {target_fqn}
            GROUP BY {partition_cols}
            HAVING COUNT(*) > 1
            LIMIT 1
        )
    """
    dup_groups = spark.sql(dup_check_sql).first()["dup_groups"]

    if dup_groups == 0:
        elapsed = time.time() - start_ts
        print(f"  [{schema}.{table}] CLEAN — no duplicates ({elapsed:.1f}s)")
        return {
            "table": f"{schema}.{table}",
            "status": "clean",
            "duplicates_removed": 0,
            "elapsed_s": round(elapsed, 1),
        }

    # Duplicates found — count before, rewrite, count after
    count_before = spark.table(target_fqn).count()

    dedup_sql = f"""
        INSERT OVERWRITE {target_fqn}
        SELECT * EXCEPT (_row_num)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {partition_cols}
                       ORDER BY {tiebreaker} DESC
                   ) AS _row_num
            FROM {target_fqn}
        )
        WHERE _row_num = 1
    """

    spark.sql(dedup_sql)

    count_after = spark.table(target_fqn).count()
    duplicates_removed = count_before - count_after
    elapsed = time.time() - start_ts

    print(f"  [{schema}.{table}] removed {duplicates_removed:,} duplicates ({count_before:,} → {count_after:,}) ({elapsed:.1f}s)")

    return {
        "table": f"{schema}.{table}",
        "status": "deduped",
        "count_before": count_before,
        "count_after": count_after,
        "duplicates_removed": duplicates_removed,
        "elapsed_s": round(elapsed, 1),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

results = []

print("=" * 70)
print(f"DEDUP STREAMING SINK TABLES  ({TARGET_CATALOG})")
print("=" * 70)

for table_key, key_config in DEDUP_KEYS.items():
    schema, table = table_key.split(".", 1)
    try:
        result = dedup_table(
            schema=schema,
            table=table,
            natural_keys=key_config["natural_keys"],
            tiebreaker=key_config["tiebreaker"],
        )
        results.append(result)
    except Exception as e:
        print(f"  [ERROR] {table_key}: {e}")
        traceback.print_exc()
        results.append({
            "table": table_key,
            "status": "failed",
            "error": str(e),
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Table Optimizations
# MAGIC
# MAGIC Enable automatic liquid clustering on all sink tables. This is idempotent
# MAGIC (no-op if already enabled) and ensures new tables get clustering automatically.

# COMMAND ----------

print()
print("=" * 70)
print("ENSURE CLUSTER BY AUTO")
print("=" * 70)

for table_key in DEDUP_KEYS:
    schema, table = table_key.split(".", 1)
    target_fqn = f"{TARGET_CATALOG}.{schema}.{table}"
    if spark.catalog.tableExists(target_fqn):
        try:
            spark.sql(f"ALTER TABLE {target_fqn} CLUSTER BY AUTO")
            print(f"  [{schema}.{table}] CLUSTER BY AUTO ensured")
        except Exception as e:
            print(f"  [{schema}.{table}] CLUSTER BY AUTO failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

deduped = [r for r in results if r["status"] == "deduped"]
clean = [r for r in results if r["status"] == "clean"]
skipped = [r for r in results if r["status"] == "skipped"]
failed = [r for r in results if r["status"] == "failed"]
total_dupes = sum(r.get("duplicates_removed", 0) for r in deduped)
total_elapsed = sum(r.get("elapsed_s", 0) for r in results)

print()
print("=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"  Tables deduped:        {len(deduped)}")
print(f"  Tables clean (no-op):  {len(clean)}")
print(f"  Tables skipped:        {len(skipped)}")
print(f"  Tables failed:         {len(failed)}")
print(f"  Total dupes removed:   {total_dupes:,}")
print(f"  Total elapsed time:    {total_elapsed:.1f}s")
print()

if failed:
    print("FAILED TABLES:")
    for r in failed:
        print(f"  - {r['table']}: {r.get('error', 'unknown')}")
    print()

print(f"{'Table':<45} {'Status':<10} {'Before':>12} {'After':>12} {'Removed':>10} {'Time':>8}")
print("-" * 100)
for r in results:
    print(
        f"{r['table']:<45} {r['status']:<10} "
        f"{r.get('count_before', '-'):>12} {r.get('count_after', '-'):>12} "
        f"{r.get('duplicates_removed', '-'):>10} {r.get('elapsed_s', '-'):>7}s"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit Value (for Workflow Alerting)

# COMMAND ----------

summary = {
    "run_timestamp": datetime.utcnow().isoformat(),
    "target_catalog": TARGET_CATALOG,
    "total_tables": len(results),
    "deduped": len(deduped),
    "clean": len(clean),
    "skipped": len(skipped),
    "failed": len(failed),
    "total_duplicates_removed": total_dupes,
    "total_elapsed_s": round(total_elapsed, 1),
    "failed_tables": [{r["table"]: r.get("error", "unknown")} for r in failed],
}

dbutils.notebook.exit(json.dumps(summary))
