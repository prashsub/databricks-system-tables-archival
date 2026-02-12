# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Archive — Batch Companion Notebook
# MAGIC
# MAGIC Handles **non-streaming system tables** that cannot use SDP Delta sinks.
# MAGIC
# MAGIC Two strategies:
# MAGIC - **Watermark MERGE** — incremental ingest with dedup for tables with a timestamp watermark.
# MAGIC - **Full Overwrite** — replace-in-place for small, slowly-changing reference tables.
# MAGIC
# MAGIC Designed for **serverless compute** (no RDD APIs, no caching).

# COMMAND ----------

import json
import time
import traceback
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("target_catalog", "system_tables_archive", "Target Catalog")
dbutils.widgets.text("tables_to_process", "all", "Tables to Process (comma-separated or 'all')")
dbutils.widgets.text("exclude_tables", "", "Tables to Exclude (comma-separated)")
dbutils.widgets.text("watermark_buffer_hours", "4", "Watermark Lookback Buffer (hours)")

# COMMAND ----------

TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TABLES_FILTER = dbutils.widgets.get("tables_to_process").strip()
WATERMARK_BUFFER_HOURS = int(dbutils.widgets.get("watermark_buffer_hours"))

# Parse exclude list — supports both "system.billing.usage" and "billing.usage"
_raw_excludes = dbutils.widgets.get("exclude_tables").strip()
EXCLUDE_TABLES = set()
if _raw_excludes:
    for _t in _raw_excludes.split(","):
        _t = _t.strip()
        if _t:
            EXCLUDE_TABLES.add(_t if _t.startswith("system.") else f"system.{_t}")

print(f"Target catalog:    {TARGET_CATALOG}")
print(f"Tables filter:     {TABLES_FILTER}")
print(f"Exclude tables:    {EXCLUDE_TABLES if EXCLUDE_TABLES else '(none)'}")
print(f"Watermark buffer:  {WATERMARK_BUFFER_HOURS} hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Definitions

# COMMAND ----------

BATCH_WATERMARK_TABLES = [
    {
        "source": "system.query.history",
        "target_schema": "query",
        "target_table": "history",
        "watermark_column": "end_time",
        "natural_keys": ["statement_id"],
    },
    {
        "source": "system.data_classification.results",
        "target_schema": "data_classification",
        "target_table": "results",
        "watermark_column": "latest_detected_time",
        "natural_keys": ["catalog_name", "schema_name", "table_name", "column_name", "class_tag"],
    },
    {
        "source": "system.access.assistant_events",
        "target_schema": "access",
        "target_table": "assistant_events",
        "watermark_column": "event_time",
        "natural_keys": ["event_id"],
    },
    {
        "source": "system.storage.predictive_optimization_operations_history",
        "target_schema": "storage",
        "target_table": "predictive_optimization_operations_history",
        "watermark_column": "start_time",
        "natural_keys": ["operation_id"],
    },
]

BATCH_OVERWRITE_TABLES = [
    # Small, slowly-changing reference tables — safe for daily full overwrite.
    {"source": "system.billing.list_prices",                       "target_schema": "billing",                "target_table": "list_prices"},
    {"source": "system.billing.account_prices",                    "target_schema": "billing",                "target_table": "account_prices"},
    {"source": "system.billing.cloud_infra_cost",                  "target_schema": "billing",                "target_table": "cloud_infra_cost"},
    {"source": "system.compute.node_types",                        "target_schema": "compute",                "target_table": "node_types"},
    {"source": "system.access.workspaces_latest",                  "target_schema": "access",                 "target_table": "workspaces_latest"},
    {"source": "system.data_quality_monitoring.table_results",     "target_schema": "data_quality_monitoring", "target_table": "table_results"},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: Ensure Target Schema Exists

# COMMAND ----------

def ensure_schema(catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy A: Watermark-Based MERGE

# COMMAND ----------

def process_watermark_table(config: dict) -> dict:
    """Incrementally MERGE new rows from source into target using a timestamp watermark."""
    source = config["source"]
    target_fqn = f"{TARGET_CATALOG}.{config['target_schema']}.{config['target_table']}"
    wm_col = config["watermark_column"]
    natural_keys = config["natural_keys"]

    ensure_schema(TARGET_CATALOG, config["target_schema"])

    start_ts = time.time()

    # Determine watermark: max(watermark_column) from target, minus buffer
    try:
        wm_row = spark.sql(
            f"SELECT max({wm_col}) AS max_wm FROM {target_fqn}"
        ).first()
        max_wm = wm_row["max_wm"] if wm_row else None
    except Exception:
        # Target table doesn't exist yet — first run
        max_wm = None

    if max_wm is not None:
        # Subtract lookback buffer for late-arriving data
        watermark = f"(CAST('{max_wm}' AS TIMESTAMP) - INTERVAL {WATERMARK_BUFFER_HOURS} HOURS)"
        source_df = spark.sql(
            f"SELECT * FROM {source} WHERE {wm_col} >= {watermark}"
        )
        print(f"  [{source}] Incremental read: {wm_col} >= {max_wm} - {WATERMARK_BUFFER_HOURS}h buffer")
    else:
        source_df = spark.table(source)
        print(f"  [{source}] First run — full read")

    source_count = source_df.count()

    if source_count == 0:
        elapsed = time.time() - start_ts
        print(f"  [{source}] No new rows. ({elapsed:.1f}s)")
        return {"table": source, "strategy": "watermark_merge", "status": "success",
                "rows_read": 0, "elapsed_s": round(elapsed, 1)}

    # Create target table if it doesn't exist
    target_exists = spark.catalog.tableExists(target_fqn)
    if not target_exists:
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_fqn)
        elapsed = time.time() - start_ts
        print(f"  [{source}] Created target with {source_count} rows. ({elapsed:.1f}s)")
        return {"table": source, "strategy": "watermark_merge", "status": "success",
                "rows_read": source_count, "elapsed_s": round(elapsed, 1), "note": "initial_load"}

    # MERGE with null-safe equality on natural keys
    source_df.createOrReplaceTempView("_source_batch")

    merge_condition = " AND ".join(
        f"target.{k} <=> source.{k}" for k in natural_keys
    )

    spark.sql(f"""
        MERGE INTO {target_fqn} AS target
        USING _source_batch AS source
        ON {merge_condition}
        WHEN NOT MATCHED THEN INSERT *
    """)

    elapsed = time.time() - start_ts
    print(f"  [{source}] Merged {source_count} rows. ({elapsed:.1f}s)")
    return {"table": source, "strategy": "watermark_merge", "status": "success",
            "rows_read": source_count, "elapsed_s": round(elapsed, 1)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy B: Full Overwrite

# COMMAND ----------

def process_overwrite_table(config: dict) -> dict:
    """Full overwrite of a small reference table."""
    source = config["source"]
    target_fqn = f"{TARGET_CATALOG}.{config['target_schema']}.{config['target_table']}"

    ensure_schema(TARGET_CATALOG, config["target_schema"])

    start_ts = time.time()

    source_df = spark.table(source)
    source_count = source_df.count()

    source_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target_fqn)

    elapsed = time.time() - start_ts
    print(f"  [{source}] Overwritten with {source_count} rows. ({elapsed:.1f}s)")
    return {"table": source, "strategy": "full_overwrite", "status": "success",
            "rows_read": source_count, "elapsed_s": round(elapsed, 1)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

def should_process(table_name: str) -> bool:
    if table_name in EXCLUDE_TABLES:
        return False
    if TABLES_FILTER == "all":
        return True
    allowed = {t.strip() for t in TABLES_FILTER.split(",")}
    return table_name in allowed

# COMMAND ----------

results = []

print("=" * 70)
print("WATERMARK MERGE TABLES")
print("=" * 70)

for cfg in BATCH_WATERMARK_TABLES:
    if not should_process(cfg["source"]):
        continue
    try:
        result = process_watermark_table(cfg)
        results.append(result)
    except Exception as e:
        print(f"  [ERROR] {cfg['source']}: {e}")
        traceback.print_exc()
        results.append({
            "table": cfg["source"],
            "strategy": "watermark_merge",
            "status": "failed",
            "error": str(e),
        })

print()
print("=" * 70)
print("FULL OVERWRITE TABLES")
print("=" * 70)

for cfg in BATCH_OVERWRITE_TABLES:
    if not should_process(cfg["source"]):
        continue
    try:
        result = process_overwrite_table(cfg)
        results.append(result)
    except Exception as e:
        print(f"  [ERROR] {cfg['source']}: {e}")
        traceback.print_exc()
        results.append({
            "table": cfg["source"],
            "strategy": "full_overwrite",
            "status": "failed",
            "error": str(e),
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

succeeded = [r for r in results if r["status"] == "success"]
failed = [r for r in results if r["status"] == "failed"]
total_rows = sum(r.get("rows_read", 0) for r in succeeded)
total_elapsed = sum(r.get("elapsed_s", 0) for r in results)

print()
print("=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"  Total tables processed:  {len(results)}")
print(f"  Succeeded:               {len(succeeded)}")
print(f"  Failed:                  {len(failed)}")
print(f"  Total rows ingested:     {total_rows:,}")
print(f"  Total elapsed time:      {total_elapsed:.1f}s")
print()

if failed:
    print("FAILED TABLES:")
    for r in failed:
        print(f"  - {r['table']}: {r.get('error', 'unknown')}")
    print()

print(f"{'Table':<55} {'Strategy':<20} {'Status':<10} {'Rows':>10} {'Time':>8}")
print("-" * 105)
for r in results:
    print(
        f"{r['table']:<55} {r['strategy']:<20} {r['status']:<10} "
        f"{r.get('rows_read', '-'):>10} {r.get('elapsed_s', '-'):>7}s"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit Value (for Workflow Alerting)

# COMMAND ----------

summary = {
    "run_timestamp": datetime.utcnow().isoformat(),
    "target_catalog": TARGET_CATALOG,
    "total_tables": len(results),
    "succeeded": len(succeeded),
    "failed": len(failed),
    "total_rows_ingested": total_rows,
    "total_elapsed_s": round(total_elapsed, 1),
    "failed_tables": [{r["table"]: r.get("error", "unknown")} for r in failed],
}

dbutils.notebook.exit(json.dumps(summary))
