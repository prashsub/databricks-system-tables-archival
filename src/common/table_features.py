"""
Delta table features that let downstream consumers read the archive incrementally.

Row tracking gives every row a stable `_metadata.row_id`; the dedup uses it to keep
the earliest-written copy of a key. Change data feed exposes row-level changes via
`table_changes()` / `readChangeFeed`.

Enabling row tracking on an existing table assigns IDs to all existing rows (a
one-time backfill in the transaction log). Never run it while another writer, such
as the streaming pipeline, is writing to the table: concurrent writes fail with
MetadataChangedException.
"""

ROW_TRACKING_PROPERTY = "delta.enableRowTracking"

FEATURE_PROPERTIES = {
    ROW_TRACKING_PROPERTY: "true",
    "delta.enableChangeDataFeed": "true",
}


def _is_true(value) -> bool:
    return str(value).strip().lower() == "true"


def row_tracking_enabled(props: dict) -> bool:
    return _is_true(props.get(ROW_TRACKING_PROPERTY, ""))


def missing_feature_properties(props: dict) -> dict:
    return {k: v for k, v in FEATURE_PROPERTIES.items() if not _is_true(props.get(k, ""))}


def build_set_tblproperties_sql(fqn: str, props: dict) -> str:
    assignments = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(props.items()))
    return f"ALTER TABLE {fqn} SET TBLPROPERTIES ({assignments})"


def read_table_properties(spark, fqn: str) -> dict:
    return {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()}


def ensure_table_features(spark, fqn: str) -> dict:
    """Enable missing feature properties. Commits nothing when all are already set."""
    missing = missing_feature_properties(read_table_properties(spark, fqn))
    if not missing:
        return {"table": fqn, "status": "present", "set": {}}
    spark.sql(build_set_tblproperties_sql(fqn, missing))
    return {"table": fqn, "status": "enabled", "set": missing}
