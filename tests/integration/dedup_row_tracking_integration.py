# Databricks notebook source
# MAGIC %md
# MAGIC # Integration test: dedup with Delta row tracking + change data feed
# MAGIC
# MAGIC Runs the production modules (`src/dedup/dedup_logic.py`,
# MAGIC `src/common/table_features.py`) against real Delta tables in a scratch schema
# MAGIC that this notebook creates (unique name) and drops afterwards.
# MAGIC
# MAGIC `run_dedup` below follows the same per-table steps as `dedup_table` in
# MAGIC `src/dedup/dedup_streaming_tables.py`: read properties, check for duplicates,
# MAGIC `INSERT OVERWRITE` with the gated ordering, verify one row per key.
# MAGIC
# MAGIC Scenarios (0 is informational: it records whether new tables get row tracking by default):
# MAGIC 1. Row-tracked table: the edited original wins over a re-append; verify passes.
# MAGIC 2. Table without row tracking: falls back to the tiebreaker, no `_metadata.row_id` error.
# MAGIC 3. `ensure_table_features` on an existing table, then a re-append: earliest copy wins;
# MAGIC    a second call commits nothing.
# MAGIC 4. Latest-state table: newer tiebreaker wins; on equal tiebreaker the earlier row wins.
# MAGIC 5. Nullable composite key: one row per key and verify passes; a doubled table fails verify.
# MAGIC 6. Enabling row tracking leaves the data files alone (`numFiles`, `sizeInBytes`).
# MAGIC 7. 1.5.0 interplay: streaming append with `mergeSchema` and `MERGE WITH SCHEMA EVOLUTION`
# MAGIC    into row-tracked + CDF tables.
# MAGIC 8. CDF is readable; a clean dedup run commits nothing.

# COMMAND ----------

import json
import sys
import traceback
import uuid

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = str(_nb_path).rsplit("/tests/", 1)[0]
if not _repo_root.startswith("/Workspace"):
    _repo_root = "/Workspace" + _repo_root
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from src.common.table_features import (
    ensure_table_features,
    read_table_properties,
    row_tracking_enabled,
)
from src.dedup.dedup_logic import (
    build_dedup_sql,
    build_dup_check_sql,
    build_verify_sql,
    check_unique,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "solution_builder", "Scratch catalog (must exist)")
dbutils.widgets.text("cleanup", "true", "Drop the scratch schema afterwards")

CATALOG = dbutils.widgets.get("catalog")
CLEANUP = dbutils.widgets.get("cleanup").strip().lower() == "true"
SCHEMA = f"sta_dedup_it_{uuid.uuid4().hex[:8]}"
BASE = f"{CATALOG}.{SCHEMA}"

# No IF NOT EXISTS: the schema must be new, so cleanup only ever drops what this run created.
spark.sql(f"CREATE SCHEMA {BASE}")
spark.sql(f"CREATE VOLUME {BASE}.chk")
print(f"Scratch schema: {BASE}")
print(f"Runtime: {spark.sql('SELECT current_version() AS v').first()['v']}")

RT_CDF = "TBLPROPERTIES (delta.enableRowTracking = true, delta.enableChangeDataFeed = true)"
# Newer runtimes can turn row tracking on for new tables by default, so tables that
# stand in for pre-1.6.0 sinks switch it off explicitly.
NO_RT = "TBLPROPERTIES (delta.enableRowTracking = false)"

results = {}
notes = {}


def record(name, passed, evidence):
    results[name] = bool(passed)
    notes[name] = evidence
    print(f"[{'PASS' if passed else 'FAIL'}] {name}: {evidence}")


def run(name, fn):
    try:
        fn()
    except Exception as e:
        record(name, False, f"EXCEPTION: {e} :: {traceback.format_exc()[-1200:]}")


def latest_version(fqn):
    return spark.sql(f"DESCRIBE HISTORY {fqn} LIMIT 1").first()["version"]


def run_dedup(fqn, natural_keys, tiebreaker, newest_version_wins=False):
    row_tracking = row_tracking_enabled(read_table_properties(spark, fqn))
    ordering = "row_id" if row_tracking else "tiebreaker"
    if spark.sql(build_dup_check_sql(fqn, natural_keys)).first()["dup_groups"] == 0:
        return {"status": "clean", "ordering": ordering}
    spark.sql(build_dedup_sql(fqn, natural_keys, tiebreaker,
                              row_tracking=row_tracking, newest_version_wins=newest_version_wins))
    verify = spark.sql(build_verify_sql(fqn, natural_keys)).first()
    check_unique(verify["total_rows"], verify["distinct_keys"], natural_keys)
    return {"status": "deduped", "ordering": ordering, "rows": verify["total_rows"]}


def tags_by_key(fqn, key="record_id"):
    return {r[key]: r["tag"] for r in spark.sql(f"SELECT {key}, tag FROM {fqn}").collect()}

# COMMAND ----------

# 0. Informational: does a new table get row tracking / CDF without asking?
_probe = f"{BASE}.s0_default_props"
spark.sql(f"CREATE TABLE {_probe} (id INT)")
_probe_props = read_table_properties(spark, _probe)
DEFAULTS = {k: _probe_props.get(k, "(unset)") for k in ("delta.enableRowTracking", "delta.enableChangeDataFeed")}
print(f"New-table defaults on this runtime: {DEFAULTS}")

# COMMAND ----------

# 1. Row-tracked table: the edited original wins over a re-append, even when the
#    re-append carries a later tiebreaker (key 3).
def scenario_1():
    t = f"{BASE}.s1_usage"
    spark.sql(f"CREATE TABLE {t} (record_id STRING, usage_date DATE, tag STRING) {RT_CDF}")
    spark.sql(f"INSERT INTO {t} VALUES ('a', DATE'2026-01-01', 'orig'), ('b', DATE'2026-01-01', 'orig'), "
              f"('c', DATE'2026-01-01', 'orig')")
    spark.sql(f"UPDATE {t} SET tag = 'edited'")
    spark.sql(f"INSERT INTO {t} VALUES ('a', DATE'2026-01-01', 'orig'), ('b', DATE'2026-01-01', 'orig'), "
              f"('c', DATE'2026-01-02', 'orig')")
    outcome = run_dedup(t, ["record_id"], "usage_date")
    survivors = tags_by_key(t)
    passed = (outcome["ordering"] == "row_id" and outcome["status"] == "deduped"
              and survivors == {"a": "edited", "b": "edited", "c": "edited"})
    record("1_row_tracked_edited_original_wins", passed, f"outcome={outcome}; survivors={survivors}")

run("1_row_tracked_edited_original_wins", scenario_1)

# COMMAND ----------

# 2. No row tracking: today's ordering (latest tiebreaker), no _metadata.row_id reference.
def scenario_2():
    t = f"{BASE}.s2_plain"
    spark.sql(f"CREATE TABLE {t} (record_id STRING, usage_date DATE, tag STRING) {NO_RT}")
    spark.sql(f"INSERT INTO {t} VALUES ('a', DATE'2026-01-01', 'older'), ('a', DATE'2026-01-02', 'newer'), "
              f"('b', DATE'2026-01-01', 'only')")
    sql = build_dedup_sql(t, ["record_id"], "usage_date", row_tracking=False)
    outcome = run_dedup(t, ["record_id"], "usage_date")
    survivors = tags_by_key(t)
    passed = (outcome["ordering"] == "tiebreaker" and "_metadata" not in sql
              and survivors == {"a": "newer", "b": "only"})
    record("2_no_row_tracking_falls_back", passed, f"outcome={outcome}; survivors={survivors}")

run("2_no_row_tracking_falls_back", scenario_2)

# COMMAND ----------

# 3. Enable features on an existing table, then edit + re-append: earliest copy wins.
#    A second ensure_table_features call commits nothing.
def scenario_3():
    t = f"{BASE}.s3_existing"
    spark.sql(f"CREATE TABLE {t} (record_id STRING, usage_date DATE, tag STRING) {NO_RT}")
    spark.sql(f"INSERT INTO {t} VALUES ('a', DATE'2026-01-01', 'orig'), ('b', DATE'2026-01-01', 'orig')")
    first = ensure_table_features(spark, t)
    spark.sql(f"UPDATE {t} SET tag = 'edited'")
    spark.sql(f"INSERT INTO {t} VALUES ('a', DATE'2026-01-01', 'orig'), ('b', DATE'2026-01-01', 'orig')")
    outcome = run_dedup(t, ["record_id"], "usage_date")
    survivors = tags_by_key(t)
    v_before = latest_version(t)
    second = ensure_table_features(spark, t)
    v_after = latest_version(t)
    passed = (first["status"] == "enabled" and "delta.enableRowTracking" in first["set"]
              and second["status"] == "present" and v_before == v_after
              and outcome["ordering"] == "row_id" and survivors == {"a": "edited", "b": "edited"})
    record("3_enable_on_existing_then_reappend", passed,
           f"first={first}; second={second}; versions={v_before}->{v_after}; outcome={outcome}; survivors={survivors}")

run("3_enable_on_existing_then_reappend", scenario_3)

# COMMAND ----------

# 4. Latest-state table (newest_version_wins): a newer update_time wins; on an equal
#    update_time the earlier row (edited original) wins.
def scenario_4():
    t = f"{BASE}.s4_latest"
    spark.sql(f"CREATE TABLE {t} (workspace_id STRING, run_id STRING, update_time TIMESTAMP, tag STRING) {RT_CDF}")
    spark.sql(f"INSERT INTO {t} VALUES ('w', 'r1', TIMESTAMP'2026-01-01 00:00:00', 'v1'), "
              f"('w', 'r2', TIMESTAMP'2026-01-01 00:00:00', 'orig')")
    spark.sql(f"UPDATE {t} SET tag = 'edited' WHERE run_id = 'r2'")
    spark.sql(f"INSERT INTO {t} VALUES ('w', 'r1', TIMESTAMP'2026-01-02 00:00:00', 'v2'), "
              f"('w', 'r2', TIMESTAMP'2026-01-01 00:00:00', 'orig')")
    outcome = run_dedup(t, ["workspace_id", "run_id"], "update_time", newest_version_wins=True)
    survivors = tags_by_key(t, key="run_id")
    passed = outcome["ordering"] == "row_id" and survivors == {"r1": "v2", "r2": "edited"}
    record("4_latest_state_newest_wins", passed, f"outcome={outcome}; survivors={survivors}")

run("4_latest_state_newest_wins", scenario_4)

# COMMAND ----------

# 5. Nullable composite key: NULL consumer_email rows dedup to one row per key and the
#    verify check passes. A deliberately doubled table fails the verify check.
def scenario_5():
    keys = ["listing_id", "event_type", "event_time", "consumer_email"]
    t = f"{BASE}.s5_marketplace"
    spark.sql(f"CREATE TABLE {t} (listing_id STRING, event_type STRING, event_time TIMESTAMP, "
              f"consumer_email STRING, tag STRING) {RT_CDF}")
    row_null = "('l1', 'view', TIMESTAMP'2026-01-01 00:00:00', NULL, 'x')"
    row_set = "('l1', 'view', TIMESTAMP'2026-01-01 00:00:00', 'p@example.com', 'x')"
    spark.sql(f"INSERT INTO {t} VALUES {row_null}, {row_set}")
    spark.sql(f"INSERT INTO {t} VALUES {row_null}, {row_set}")
    outcome = run_dedup(t, keys, "event_time")
    rows = spark.sql(f"SELECT count(*) AS c FROM {t}").first()["c"]

    doubled = f"{BASE}.s5_doubled"
    spark.sql(f"CREATE TABLE {doubled} AS SELECT * FROM {t} UNION ALL SELECT * FROM {t}")
    v = spark.sql(build_verify_sql(doubled, keys)).first()
    try:
        check_unique(v["total_rows"], v["distinct_keys"], keys)
        verify_raised = False
    except RuntimeError:
        verify_raised = True

    passed = outcome["status"] == "deduped" and rows == 2 and verify_raised
    record("5_nullable_composite_key", passed,
           f"outcome={outcome}; rows_after={rows}; doubled_verify={dict(v.asDict())}; raised={verify_raised}")

run("5_nullable_composite_key", scenario_5)

# COMMAND ----------

# 6. Enabling row tracking on an existing table leaves the data files unchanged.
def scenario_6():
    t = f"{BASE}.s6_backfill"
    spark.sql(f"CREATE TABLE {t} (record_id BIGINT, tag STRING) {NO_RT}")
    for i in range(3):
        spark.sql(f"INSERT INTO {t} SELECT id + {i * 1000}, 'x' FROM range(1000)")
    before = spark.sql(f"DESCRIBE DETAIL {t}").first()
    outcome = ensure_table_features(spark, t)
    after = spark.sql(f"DESCRIBE DETAIL {t}").first()
    row_ids = spark.sql(f"SELECT count(DISTINCT _metadata.row_id) AS n, count(*) AS c FROM {t}").first()
    passed = (outcome["status"] == "enabled" and "delta.enableRowTracking" in outcome["set"]
              and before["numFiles"] == after["numFiles"]
              and before["sizeInBytes"] == after["sizeInBytes"]
              and row_ids["n"] == row_ids["c"] == 3000)
    record("6_backfill_leaves_files", passed,
           f"numFiles {before['numFiles']}->{after['numFiles']}; sizeInBytes {before['sizeInBytes']}->"
           f"{after['sizeInBytes']}; distinct_row_ids={row_ids['n']}/{row_ids['c']}")

run("6_backfill_leaves_files", scenario_6)

# COMMAND ----------

# 7. 1.5.0 interplay: schema-evolving writes into row-tracked + CDF tables.
def scenario_7():
    src = f"{BASE}.s7_src"
    sink = f"{BASE}.s7_sink"
    merge_tgt = f"{BASE}.s7_merge"
    ckpt = f"/Volumes/{CATALOG}/{SCHEMA}/chk/s7"
    spark.sql(f"CREATE TABLE {src} (record_id STRING, tag STRING, new_col STRING)")
    spark.sql(f"INSERT INTO {src} VALUES ('a', 'x', 'n1'), ('b', 'x', 'n2')")

    spark.sql(f"CREATE TABLE {sink} (record_id STRING, tag STRING) {RT_CDF}")
    (spark.readStream.option("skipChangeCommits", "true").table(src)
        .writeStream.option("checkpointLocation", ckpt).option("mergeSchema", "true")
        .trigger(availableNow=True).toTable(sink)
        .awaitTermination())
    sink_cols = spark.table(sink).columns
    sink_rows = spark.sql(f"SELECT count(*) AS c, count(_metadata.row_id) AS r FROM {sink}").first()
    sink_ok = ("new_col" in sink_cols and sink_rows["c"] == 2 and sink_rows["r"] == 2
               and row_tracking_enabled(read_table_properties(spark, sink)))

    spark.sql(f"CREATE TABLE {merge_tgt} (record_id STRING, tag STRING) {RT_CDF}")
    spark.sql(f"INSERT INTO {merge_tgt} VALUES ('a', 'old')")
    spark.sql(f"""
        MERGE WITH SCHEMA EVOLUTION INTO {merge_tgt} AS target
        USING {src} AS source
        ON target.record_id <=> source.record_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    merge_cols = spark.table(merge_tgt).columns
    merge_rows = spark.sql(f"SELECT count(*) AS c FROM {merge_tgt}").first()["c"]
    merge_ok = ("new_col" in merge_cols and merge_rows == 2
                and row_tracking_enabled(read_table_properties(spark, merge_tgt)))

    record("7_schema_evolution_with_row_tracking", sink_ok and merge_ok,
           f"sink_cols={sink_cols}; sink_rows={sink_rows.asDict()}; merge_cols={merge_cols}; merge_rows={merge_rows}")

run("7_schema_evolution_with_row_tracking", scenario_7)

# COMMAND ----------

# 8. CDF is readable after appends and a dedup; a clean dedup run commits nothing.
def scenario_8():
    t = f"{BASE}.s8_cdf"
    spark.sql(f"CREATE TABLE {t} (record_id STRING, usage_date DATE, tag STRING) {RT_CDF}")
    start = latest_version(t)
    spark.sql(f"INSERT INTO {t} VALUES ('a', DATE'2026-01-01', 'x'), ('b', DATE'2026-01-01', 'x')")
    spark.sql(f"INSERT INTO {t} VALUES ('a', DATE'2026-01-01', 'x')")
    run_dedup(t, ["record_id"], "usage_date")
    changes = {r["_change_type"]: r["n"] for r in spark.sql(
        f"SELECT _change_type, count(*) AS n FROM table_changes('{t}', {start + 1}) GROUP BY _change_type"
    ).collect()}

    v_before = latest_version(t)
    clean = run_dedup(t, ["record_id"], "usage_date")
    v_after = latest_version(t)
    passed = changes.get("insert", 0) >= 3 and clean["status"] == "clean" and v_before == v_after
    record("8_cdf_readable_clean_run_no_commit", passed,
           f"changes={changes}; clean={clean}; versions={v_before}->{v_after}")

run("8_cdf_readable_clean_run_no_commit", scenario_8)

# COMMAND ----------

if CLEANUP:
    spark.sql(f"DROP SCHEMA {BASE} CASCADE")
    print(f"Dropped {BASE}")

summary = {
    "schema": BASE,
    "cleaned_up": CLEANUP,
    "new_table_defaults": DEFAULTS,
    "passed": sum(1 for v in results.values() if v),
    "total": len(results),
    "results": results,
    "notes": notes,
}
print(json.dumps(summary, indent=2, default=str))

failed = [k for k, v in results.items() if not v]
if failed:
    raise AssertionError(f"Integration scenarios failed: {', '.join(failed)}")

dbutils.notebook.exit(json.dumps(summary, default=str))
