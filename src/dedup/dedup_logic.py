"""
Natural key registry and SQL builders for deduplicating the streaming sink tables.

Used by the `dedup_streaming_tables` notebook. Kept free of Spark imports so the
logic can be unit tested locally.

Ordering when a key has several copies:
- Row tracking off: `<tiebreaker> DESC` (behavior before 1.6.0).
- Row tracking on: `_metadata.row_id ASC, <tiebreaker> DESC`. The earliest-written
  copy wins, so a re-append after a checkpoint reset never displaces an archived
  row, including rows edited downstream.
- Row tracking on, `newest_version_wins`: `<tiebreaker> DESC, _metadata.row_id ASC`.
  For tables that hold the latest state of an entity, a newer version still wins.
"""

import traceback

ROW_ID_COLUMN = "_metadata.row_id"

# Sources: Databricks docs, `information_schema.columns`, `DESCRIBE` output.
# Streaming tiebreakers are mirrored in FRESHNESS_COLUMNS (src/monitoring/freshness_check.py).
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
        "newest_version_wins": True,
    },
    "mlflow.runs_latest": {
        "natural_keys": ["workspace_id", "run_id"],
        "tiebreaker": "update_time",
        "newest_version_wins": True,
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
        "newest_version_wins": True,
    },
    "lakeflow.zerobus_ingest": {
        "natural_keys": ["stream_id", "commit_version"],
        "tiebreaker": "commit_time",
    },
}


def order_by_clause(tiebreaker: str, row_tracking: bool, newest_version_wins: bool = False) -> str:
    if not row_tracking:
        return f"{tiebreaker} DESC"
    if newest_version_wins:
        return f"{tiebreaker} DESC, {ROW_ID_COLUMN} ASC"
    return f"{ROW_ID_COLUMN} ASC, {tiebreaker} DESC"


def build_dup_check_sql(target_fqn: str, natural_keys: list) -> str:
    partition_cols = ", ".join(natural_keys)
    return f"""
        SELECT COUNT(*) AS dup_groups FROM (
            SELECT {partition_cols}
            FROM {target_fqn}
            GROUP BY {partition_cols}
            HAVING COUNT(*) > 1
            LIMIT 1
        )
    """


def build_dedup_sql(target_fqn: str, natural_keys: list, tiebreaker: str,
                    row_tracking: bool = False, newest_version_wins: bool = False) -> str:
    partition_cols = ", ".join(natural_keys)
    order_by = order_by_clause(tiebreaker, row_tracking, newest_version_wins)
    return f"""
        INSERT OVERWRITE {target_fqn}
        SELECT * EXCEPT (_row_num)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {partition_cols}
                       ORDER BY {order_by}
                   ) AS _row_num
            FROM {target_fqn}
        )
        WHERE _row_num = 1
    """


def build_verify_sql(target_fqn: str, natural_keys: list) -> str:
    # struct(...) keeps rows whose key columns contain NULLs; a multi-column
    # COUNT(DISTINCT a, b) would skip them and report a false mismatch.
    return (
        f"SELECT COUNT(*) AS total_rows, "
        f"COUNT(DISTINCT struct({', '.join(natural_keys)})) AS distinct_keys "
        f"FROM {target_fqn}"
    )


def check_unique(total_rows: int, distinct_keys: int, natural_keys: list) -> None:
    if total_rows != distinct_keys:
        raise RuntimeError(
            f"post-dedup verification failed: {total_rows:,} rows vs "
            f"{distinct_keys:,} distinct keys ({', '.join(natural_keys)})"
        )


def process_all(dedup_keys: dict, dedup_fn) -> list:
    """Run dedup_fn for every table. A failing table is recorded and the loop continues."""
    results = []
    for table_key, key_config in dedup_keys.items():
        schema, table = table_key.split(".", 1)
        try:
            results.append(dedup_fn(
                schema=schema,
                table=table,
                natural_keys=key_config["natural_keys"],
                tiebreaker=key_config["tiebreaker"],
                newest_version_wins=key_config.get("newest_version_wins", False),
            ))
        except Exception as e:
            print(f"  [ERROR] {table_key}: {e}")
            traceback.print_exc()
            results.append({"table": table_key, "status": "failed", "error": str(e)})
    return results


def failed_results(results: list) -> list:
    return [r for r in results if r["status"] == "failed"]


def raise_if_failed(results: list) -> None:
    failed = failed_results(results)
    if failed:
        details = "; ".join(f"{r['table']}: {r.get('error', 'unknown')}" for r in failed)
        raise RuntimeError(f"Dedup failed for {len(failed)} table(s): {details}")
