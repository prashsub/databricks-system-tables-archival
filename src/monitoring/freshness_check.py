# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Archive — Per-Table Freshness Check
# MAGIC
# MAGIC Fails (and triggers the job's failure email) if any incrementally archived
# MAGIC table has fallen behind its source system table by more than the threshold.
# MAGIC
# MAGIC For each table, the check compares the archive's latest timestamp against the
# MAGIC **source**, not against wall-clock time, so quiet tables that legitimately have
# MAGIC no new rows never raise a false alarm:
# MAGIC ```
# MAGIC archive_max = max(<time_col>) FROM <archive>
# MAGIC STALE if the source has any row with <time_col> > archive_max + threshold
# MAGIC ```
# MAGIC
# MAGIC The 6 full-overwrite reference tables are not checked — they are rewritten daily.
# MAGIC
# MAGIC The threshold (default 48h) leaves 5 days to remediate before the 168-hour
# MAGIC Delta Sharing VACUUM window makes a stalled stream unrecoverable.

# COMMAND ----------

import time
from datetime import date, datetime, timedelta

# COMMAND ----------

dbutils.widgets.text("target_catalog", "system_tables_archive", "Target Catalog")
dbutils.widgets.text("exclude_tables", "", "Tables to Exclude (comma-separated)")
dbutils.widgets.text("stale_threshold_hours", "48", "Stale Threshold (hours)")

# COMMAND ----------

TARGET_CATALOG = dbutils.widgets.get("target_catalog")
STALE_THRESHOLD_HOURS = int(dbutils.widgets.get("stale_threshold_hours"))

# Parse exclude list — supports both "system.billing.usage" and "billing.usage"
_raw_excludes = dbutils.widgets.get("exclude_tables").strip()
EXCLUDE_TABLES = set()
if _raw_excludes:
    for _t in _raw_excludes.split(","):
        _t = _t.strip()
        if _t:
            EXCLUDE_TABLES.add(_t if _t.startswith("system.") else f"system.{_t}")

print(f"Target catalog:    {TARGET_CATALOG}")
print(f"Stale threshold:   {STALE_THRESHOLD_HOURS} hours")
print(f"Exclude tables:    {EXCLUDE_TABLES if EXCLUDE_TABLES else '(none)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Freshness Registry
# MAGIC
# MAGIC Archive table (`schema.table`) → time column. The source is always
# MAGIC `system.<schema>.<table>`.
# MAGIC
# MAGIC Streaming time columns mirror the `tiebreaker` values in `DEDUP_KEYS`
# MAGIC (`src/dedup/dedup_streaming_tables.py`); watermark time columns mirror
# MAGIC `BATCH_WATERMARK_TABLES` (`src/batch/batch_companion.py`). Keep them in sync
# MAGIC when adding tables.

# COMMAND ----------

FRESHNESS_COLUMNS = {
    # --- Streaming sink tables (27) ---
    "access.audit": "event_time",
    "access.column_lineage": "event_time",
    "access.table_lineage": "event_time",
    "access.clean_room_events": "event_time",
    "access.inbound_network": "event_time",
    "access.outbound_network": "event_time",
    "billing.usage": "usage_date",
    "serving.endpoint_usage": "request_time",
    "sharing.materialization_history": "created_at",
    "mlflow.run_metrics_history": "insert_time",
    "mlflow.experiments_latest": "update_time",
    "mlflow.runs_latest": "update_time",
    "compute.clusters": "change_time",
    "compute.warehouses": "change_time",
    "lakeflow.jobs": "change_time",
    "lakeflow.job_tasks": "change_time",
    "lakeflow.pipelines": "change_time",
    "serving.served_entities": "change_time",
    "lakeflow.job_run_timeline": "period_start_time",
    "lakeflow.job_task_run_timeline": "period_start_time",
    "lakeflow.pipeline_update_timeline": "period_start_time",
    "compute.warehouse_events": "event_time",
    "marketplace.listing_funnel_events": "event_time",
    "marketplace.listing_access_events": "event_time",
    "compute.node_timeline": "start_time",
    "lakeflow.zerobus_stream": "event_time",
    "lakeflow.zerobus_ingest": "commit_time",
    # --- Batch watermark MERGE tables (4) ---
    "query.history": "end_time",
    "data_classification.results": "latest_detected_time",
    "access.assistant_events": "event_time",
    "storage.predictive_optimization_operations_history": "start_time",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Logic

# COMMAND ----------

def _as_datetime(value):
    """Normalize a max() result (TIMESTAMP or DATE) to a datetime."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return value


def _source_has_rows(source: str, time_col: str, after=None) -> bool:
    """True if the source has any row (optionally only rows with time_col > after)."""
    if after is None:
        return spark.sql(f"SELECT 1 FROM {source} LIMIT 1").first() is not None
    return spark.sql(
        f"SELECT 1 FROM {source} WHERE {time_col} > :after LIMIT 1",
        args={"after": after},
    ).first() is not None


def check_table(table_key: str, time_col: str) -> dict:
    source = f"system.{table_key}"
    archive = f"{TARGET_CATALOG}.{table_key}"
    result = {"table": table_key, "time_col": time_col, "archive_max": None}

    if source in EXCLUDE_TABLES:
        return {**result, "status": "SKIPPED", "reason": "excluded"}

    try:
        if not spark.catalog.tableExists(archive):
            if _source_has_rows(source, time_col):
                return {**result, "status": "STALE", "reason": "archive table missing; source has data"}
            return {**result, "status": "OK", "reason": "archive missing; source empty"}

        archive_max = _as_datetime(
            spark.sql(f"SELECT max({time_col}) AS m FROM {archive}").first()["m"]
        )
        result["archive_max"] = archive_max

        if archive_max is None:
            if _source_has_rows(source, time_col):
                return {**result, "status": "STALE", "reason": "archive empty; source has data"}
            return {**result, "status": "OK", "reason": "archive and source empty"}

        cutoff = archive_max + timedelta(hours=STALE_THRESHOLD_HOURS)
        if _source_has_rows(source, time_col, after=cutoff):
            return {
                **result,
                "status": "STALE",
                "reason": f"source has rows newer than archive max + {STALE_THRESHOLD_HOURS}h",
            }
        return {**result, "status": "OK", "reason": ""}
    except Exception as e:
        msg = str(e).splitlines()[0][:200]
        if "TABLE_OR_VIEW_NOT_FOUND" in msg and source in msg:
            return {**result, "status": "SKIPPED", "reason": "source not available in this workspace"}
        return {**result, "status": "ERROR", "reason": msg}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

print("=" * 70)
print(f"FRESHNESS CHECK  ({TARGET_CATALOG}, threshold {STALE_THRESHOLD_HOURS}h)")
print("=" * 70)

start_ts = time.time()
results = [check_table(k, c) for k, c in FRESHNESS_COLUMNS.items()]

for r in results:
    archive_max = r["archive_max"].isoformat(sep=" ") if r["archive_max"] else "-"
    print(f"  {r['status']:<8} {r['table']:<52} max={archive_max:<26} {r['reason']}")

failing = [r for r in results if r["status"] in ("STALE", "ERROR")]
counts = {s: sum(1 for r in results if r["status"] == s) for s in ("OK", "STALE", "ERROR", "SKIPPED")}

print()
print(f"  OK: {counts['OK']}  STALE: {counts['STALE']}  ERROR: {counts['ERROR']}  "
      f"SKIPPED: {counts['SKIPPED']}  ({time.time() - start_ts:.1f}s)")

# COMMAND ----------

if failing:
    details = "; ".join(f"{r['table']} [{r['status']}: {r['reason']}]" for r in failing)
    raise RuntimeError(
        f"{len(failing)} archive table(s) failed the freshness check "
        f"(threshold {STALE_THRESHOLD_HOURS}h, VACUUM window 168h): {details}"
    )

print("All archive tables are fresh.")
