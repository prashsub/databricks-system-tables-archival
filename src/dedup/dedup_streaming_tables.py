# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Archive — Deduplicate Streaming Sink Tables
# MAGIC
# MAGIC After a **Full Refresh** of the streaming pipeline, Delta sinks accumulate
# MAGIC duplicate rows (the pipeline re-reads the full source and appends again).
# MAGIC
# MAGIC This notebook removes duplicates from all 27 streaming sink tables, keeping one
# MAGIC row per natural key with `ROW_NUMBER() OVER (PARTITION BY <natural_keys> ...) = 1`:
# MAGIC
# MAGIC | Table state | Ordering | Surviving copy |
# MAGIC |---|---|---|
# MAGIC | Row tracking off | `<tiebreaker> DESC` | Latest tiebreaker (behavior before 1.6.0) |
# MAGIC | Row tracking on | `_metadata.row_id ASC, <tiebreaker> DESC` | Earliest-written copy, so downstream edits survive a re-append |
# MAGIC | Row tracking on, latest-state table | `<tiebreaker> DESC, _metadata.row_id ASC` | Newest version; earliest copy on ties |
# MAGIC
# MAGIC **Safety**: Each table is overwritten atomically with `INSERT OVERWRITE`, then
# MAGIC checked for one row per key. If the notebook fails mid-way, already-processed
# MAGIC tables are clean and unprocessed tables still have their (duplicated) data intact.
# MAGIC
# MAGIC **Failure**: every table is attempted; the task then fails, naming each table
# MAGIC that could not be deduplicated or verified.
# MAGIC
# MAGIC **Maintenance**: afterwards each sink gets `CLUSTER BY AUTO`, plus row tracking
# MAGIC and change data feed if they are not on yet. This runs after the pipeline has
# MAGIC finished, because enabling row tracking must not overlap other writes.

# COMMAND ----------

import json
import sys
import time
from datetime import datetime

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = str(_nb_path).rsplit("/src/", 1)[0]
if not _bundle_root.startswith("/Workspace"):
    _bundle_root = "/Workspace" + _bundle_root
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from src.common.table_features import ensure_table_features, read_table_properties, row_tracking_enabled
from src.dedup.dedup_logic import (
    DEDUP_KEYS,
    build_dedup_sql,
    build_dup_check_sql,
    build_verify_sql,
    check_unique,
    failed_results,
    process_all,
    raise_if_failed,
)

# COMMAND ----------

dbutils.widgets.text("target_catalog", "system_tables_archive", "Target Catalog")

# COMMAND ----------

TARGET_CATALOG = dbutils.widgets.get("target_catalog")
print(f"Target catalog: {TARGET_CATALOG}")
print(f"Tables in registry: {len(DEDUP_KEYS)} (src/dedup/dedup_logic.py)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dedup Logic
# MAGIC
# MAGIC The natural key registry (`DEDUP_KEYS`) and the SQL builders live in
# MAGIC `src/dedup/dedup_logic.py`. Each entry defines the natural key (dedup partition
# MAGIC columns) and the tiebreaker column used for ordering.

# COMMAND ----------

def dedup_table(schema: str, table: str, natural_keys: list, tiebreaker: str,
                newest_version_wins: bool = False) -> dict:
    """Remove duplicates from a single sink table using ROW_NUMBER window function.

    Optimization: checks for duplicate key groups first. If none exist, skips the
    expensive INSERT OVERWRITE entirely. This reduces steady-state runtime from
    ~15 min (full rewrite of all tables) to ~3-5 min (scan-only).

    Uses INSERT OVERWRITE for atomic replacement — either the full dedup succeeds
    or the table remains unchanged. After the rewrite the table must hold exactly
    one row per natural key; otherwise this raises.
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

    row_tracking = row_tracking_enabled(read_table_properties(spark, target_fqn))
    ordering = "row_id" if row_tracking else "tiebreaker"

    # Check for duplicates before rewriting — GROUP BY + HAVING is much cheaper
    # than INSERT OVERWRITE when no duplicates exist (scan-only, no shuffle/write).
    dup_groups = spark.sql(build_dup_check_sql(target_fqn, natural_keys)).first()["dup_groups"]

    if dup_groups == 0:
        elapsed = time.time() - start_ts
        print(f"  [{schema}.{table}] CLEAN — no duplicates ({elapsed:.1f}s)")
        return {
            "table": f"{schema}.{table}",
            "status": "clean",
            "ordering": ordering,
            "duplicates_removed": 0,
            "elapsed_s": round(elapsed, 1),
        }

    # Duplicates found — count before, rewrite, verify, count after
    count_before = spark.table(target_fqn).count()

    spark.sql(build_dedup_sql(target_fqn, natural_keys, tiebreaker,
                              row_tracking=row_tracking, newest_version_wins=newest_version_wins))

    verify = spark.sql(build_verify_sql(target_fqn, natural_keys)).first()
    check_unique(verify["total_rows"], verify["distinct_keys"], natural_keys)

    count_after = verify["total_rows"]
    duplicates_removed = count_before - count_after
    elapsed = time.time() - start_ts

    print(f"  [{schema}.{table}] removed {duplicates_removed:,} duplicates ({count_before:,} → {count_after:,}) "
          f"ordering={ordering} ({elapsed:.1f}s)")

    return {
        "table": f"{schema}.{table}",
        "status": "deduped",
        "ordering": ordering,
        "count_before": count_before,
        "count_after": count_after,
        "duplicates_removed": duplicates_removed,
        "elapsed_s": round(elapsed, 1),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution

# COMMAND ----------

print("=" * 70)
print(f"DEDUP STREAMING SINK TABLES  ({TARGET_CATALOG})")
print("=" * 70)

results = process_all(DEDUP_KEYS, dedup_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Table Optimizations
# MAGIC
# MAGIC Enable automatic liquid clustering on all sink tables, and turn on row tracking
# MAGIC and change data feed where they are missing. Both are idempotent; table
# MAGIC properties are only written when something is missing, so a steady-state run
# MAGIC commits nothing for them.
# MAGIC
# MAGIC The first time row tracking is enabled on a table, Delta assigns row IDs to the
# MAGIC existing rows (recorded in the transaction log). A failure here is reported as a
# MAGIC warning and does not fail the task: a table without row tracking keeps the
# MAGIC tiebreaker ordering. Tables whose dedup failed in this run are skipped, so row
# MAGIC tracking is only ever enabled on a table without duplicates.

# COMMAND ----------

print()
print("=" * 70)
print("ENSURE CLUSTER BY AUTO + ROW TRACKING + CHANGE DATA FEED")
print("=" * 70)

feature_warnings = []
# Row tracking backfills IDs onto every copy present, so it can't tell duplicates
# apart that exist when it is enabled. Tables still holding duplicates wait for a clean run.
dedup_failed = {r["table"] for r in failed_results(results)}

for table_key in DEDUP_KEYS:
    schema, table = table_key.split(".", 1)
    target_fqn = f"{TARGET_CATALOG}.{schema}.{table}"
    if spark.catalog.tableExists(target_fqn):
        try:
            spark.sql(f"ALTER TABLE {target_fqn} CLUSTER BY AUTO")
            print(f"  [{schema}.{table}] CLUSTER BY AUTO ensured")
        except Exception as e:
            print(f"  [{schema}.{table}] CLUSTER BY AUTO failed: {e}")
        if table_key in dedup_failed:
            print(f"  [{schema}.{table}] row tracking / CDF not checked: dedup failed this run")
            continue
        try:
            outcome = ensure_table_features(spark, target_fqn)
            if outcome["status"] == "enabled":
                print(f"  [{schema}.{table}] enabled {', '.join(sorted(outcome['set']))}")
        except Exception as e:
            print(f"  [{schema}.{table}] WARNING: enabling row tracking / CDF failed: {e}")
            feature_warnings.append({table_key: str(e)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

deduped = [r for r in results if r["status"] == "deduped"]
clean = [r for r in results if r["status"] == "clean"]
skipped = [r for r in results if r["status"] == "skipped"]
failed = failed_results(results)
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
print(f"  Feature warnings:      {len(feature_warnings)}")
print()

if failed:
    print("FAILED TABLES:")
    for r in failed:
        print(f"  - {r['table']}: {r.get('error', 'unknown')}")
    print()

print(f"{'Table':<45} {'Status':<10} {'Ordering':<10} {'Before':>12} {'After':>12} {'Removed':>10} {'Time':>8}")
print("-" * 111)
for r in results:
    print(
        f"{r['table']:<45} {r['status']:<10} {r.get('ordering', '-'):<10} "
        f"{r.get('count_before', '-'):>12} {r.get('count_after', '-'):>12} "
        f"{r.get('duplicates_removed', '-'):>10} {r.get('elapsed_s', '-'):>7}s"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit Value (for Workflow Alerting)
# MAGIC
# MAGIC If any table failed, the task fails here (after the summary is printed) so the
# MAGIC run is marked failed and the task's failure notification fires.

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
    "feature_warnings": feature_warnings,
}
print(json.dumps(summary, indent=2))

raise_if_failed(results)

dbutils.notebook.exit(json.dumps(summary))
