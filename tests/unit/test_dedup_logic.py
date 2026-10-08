import ast
import re
from pathlib import Path

import pytest

from src.dedup.dedup_logic import (
    DEDUP_KEYS,
    build_dedup_sql,
    build_dup_check_sql,
    build_verify_sql,
    check_unique,
    order_by_clause,
    process_all,
    raise_if_failed,
)

REPO = Path(__file__).resolve().parents[2]

# Registry as it stood in src/dedup/dedup_streaming_tables.py before the move.
PRE_MOVE_REGISTRY = {
    "access.audit": (["event_id"], "event_time"),
    "access.column_lineage": (["record_id"], "event_time"),
    "access.table_lineage": (["record_id"], "event_time"),
    "access.clean_room_events": (["event_id"], "event_time"),
    "access.inbound_network": (["event_id"], "event_time"),
    "access.outbound_network": (["event_id"], "event_time"),
    "billing.usage": (["record_id"], "usage_date"),
    "serving.endpoint_usage": (["databricks_request_id"], "request_time"),
    "sharing.materialization_history": (["sharing_materialization_id"], "created_at"),
    "mlflow.run_metrics_history": (["record_id"], "insert_time"),
    "mlflow.experiments_latest": (["workspace_id", "experiment_id"], "update_time"),
    "mlflow.runs_latest": (["workspace_id", "run_id"], "update_time"),
    "compute.clusters": (["workspace_id", "cluster_id", "change_time"], "change_time"),
    "compute.warehouses": (["workspace_id", "warehouse_id", "change_time"], "change_time"),
    "lakeflow.jobs": (["workspace_id", "job_id", "change_time"], "change_time"),
    "lakeflow.job_tasks": (["workspace_id", "job_id", "task_key", "change_time"], "change_time"),
    "lakeflow.pipelines": (["workspace_id", "pipeline_id", "change_time"], "change_time"),
    "serving.served_entities": (["served_entity_id", "change_time"], "change_time"),
    "lakeflow.job_run_timeline": (["workspace_id", "run_id", "period_start_time"], "period_start_time"),
    "lakeflow.job_task_run_timeline": (["workspace_id", "run_id", "period_start_time"], "period_start_time"),
    "lakeflow.pipeline_update_timeline": (["workspace_id", "update_id", "period_start_time"], "period_start_time"),
    "compute.warehouse_events": (["workspace_id", "warehouse_id", "event_type", "event_time"], "event_time"),
    "marketplace.listing_funnel_events": (
        ["listing_id", "event_type", "event_time", "consumer_cloud", "consumer_region"], "event_time"),
    "marketplace.listing_access_events": (
        ["listing_id", "event_type", "event_time", "consumer_email"], "event_time"),
    "compute.node_timeline": (["cluster_id", "instance_id", "start_time"], "start_time"),
    "lakeflow.zerobus_stream": (["stream_id"], "event_time"),
    "lakeflow.zerobus_ingest": (["stream_id", "commit_version"], "commit_time"),
}

# SQL the notebook ran before this change (dedup_streaming_tables.py, 1.5.0).
PRE_CHANGE_DEDUP_SQL = """
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

PRE_CHANGE_DUP_CHECK_SQL = """
        SELECT COUNT(*) AS dup_groups FROM (
            SELECT {partition_cols}
            FROM {target_fqn}
            GROUP BY {partition_cols}
            HAVING COUNT(*) > 1
            LIMIT 1
        )
"""


def _norm(sql):
    return re.sub(r"\s+", " ", sql).strip()


def _literal_assignment(path, name):
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {path}")


# --- registry -----------------------------------------------------------------

def test_registry_unchanged_by_move():
    assert set(DEDUP_KEYS) == set(PRE_MOVE_REGISTRY)
    assert len(DEDUP_KEYS) == 27
    for table, (keys, tiebreaker) in PRE_MOVE_REGISTRY.items():
        assert DEDUP_KEYS[table]["natural_keys"] == keys, table
        assert DEDUP_KEYS[table]["tiebreaker"] == tiebreaker, table


def test_newest_version_wins_exactly_latest_state_tables():
    flagged = {t for t, c in DEDUP_KEYS.items() if c.get("newest_version_wins")}
    assert flagged == {"mlflow.experiments_latest", "mlflow.runs_latest", "lakeflow.zerobus_stream"}


def test_registry_matches_streaming_tables():
    streaming = _literal_assignment(
        REPO / "src/streaming_etl/transformations/streaming_archive.py", "STREAMING_TABLES")
    assert {f"{c['schema']}.{c['table']}" for c in streaming} == set(DEDUP_KEYS)


def test_tiebreakers_match_freshness_columns():
    freshness = _literal_assignment(REPO / "src/monitoring/freshness_check.py", "FRESHNESS_COLUMNS")
    for table, cfg in DEDUP_KEYS.items():
        assert freshness.get(table) == cfg["tiebreaker"], table


def test_notebook_no_longer_defines_registry():
    tree = ast.parse((REPO / "src/dedup/dedup_streaming_tables.py").read_text())
    assigned = {t.id for n in tree.body if isinstance(n, ast.Assign)
                for t in n.targets if isinstance(t, ast.Name)}
    assert "DEDUP_KEYS" not in assigned


# --- ordering and SQL ---------------------------------------------------------

def test_order_by_without_row_tracking_is_todays_ordering():
    assert order_by_clause("usage_date", row_tracking=False) == "usage_date DESC"
    assert order_by_clause("update_time", row_tracking=False, newest_version_wins=True) == "update_time DESC"


def test_order_by_row_tracking_earliest_written_first():
    assert order_by_clause("usage_date", row_tracking=True) == "_metadata.row_id ASC, usage_date DESC"


def test_order_by_row_tracking_latest_state_keeps_newest():
    assert (order_by_clause("update_time", row_tracking=True, newest_version_wins=True)
            == "update_time DESC, _metadata.row_id ASC")


def test_dedup_sql_without_row_tracking_matches_pre_change_sql():
    for table, (keys, tiebreaker) in PRE_MOVE_REGISTRY.items():
        fqn = f"cat.{table}"
        expected = PRE_CHANGE_DEDUP_SQL.format(
            target_fqn=fqn, partition_cols=", ".join(keys), tiebreaker=tiebreaker)
        assert _norm(build_dedup_sql(fqn, keys, tiebreaker, row_tracking=False)) == _norm(expected), table


def test_dedup_sql_with_row_tracking_only_changes_order_by():
    sql = build_dedup_sql("cat.billing.usage", ["record_id"], "usage_date", row_tracking=True)
    expected = PRE_CHANGE_DEDUP_SQL.format(
        target_fqn="cat.billing.usage", partition_cols="record_id", tiebreaker="usage_date",
    ).replace("ORDER BY usage_date DESC", "ORDER BY _metadata.row_id ASC, usage_date DESC")
    assert _norm(sql) == _norm(expected)


def test_dup_check_sql_unchanged():
    keys = ["workspace_id", "run_id"]
    expected = PRE_CHANGE_DUP_CHECK_SQL.format(target_fqn="c.s.t", partition_cols="workspace_id, run_id")
    assert _norm(build_dup_check_sql("c.s.t", keys)) == _norm(expected)


def test_verify_sql_is_null_safe_on_composite_keys():
    sql = build_verify_sql("c.s.t", ["listing_id", "consumer_email"])
    assert _norm(sql) == (
        "SELECT COUNT(*) AS total_rows, COUNT(DISTINCT struct(listing_id, consumer_email)) "
        "AS distinct_keys FROM c.s.t"
    )


def test_check_unique():
    check_unique(10, 10, ["record_id"])
    with pytest.raises(RuntimeError, match=r"12 rows vs 10 distinct keys \(record_id\)"):
        check_unique(12, 10, ["record_id"])


# --- failure handling ---------------------------------------------------------

THREE_TABLES = {
    "a.one": {"natural_keys": ["id"], "tiebreaker": "ts"},
    "b.two": {"natural_keys": ["id"], "tiebreaker": "ts", "newest_version_wins": True},
    "c.three": {"natural_keys": ["id"], "tiebreaker": "ts"},
}


def test_process_all_continues_after_a_failure_and_then_raises():
    calls = []

    def fake_dedup(schema, table, natural_keys, tiebreaker, newest_version_wins):
        calls.append((f"{schema}.{table}", newest_version_wins))
        if table == "two":
            raise ValueError("boom")
        return {"table": f"{schema}.{table}", "status": "deduped"}

    results = process_all(THREE_TABLES, fake_dedup)

    assert calls == [("a.one", False), ("b.two", True), ("c.three", False)]
    assert [r["status"] for r in results] == ["deduped", "failed", "deduped"]
    assert results[1] == {"table": "b.two", "status": "failed", "error": "boom"}
    with pytest.raises(RuntimeError, match=r"Dedup failed for 1 table\(s\): b\.two: boom"):
        raise_if_failed(results)


def test_raise_if_failed_names_every_failed_table():
    results = [
        {"table": "a.one", "status": "failed", "error": "x"},
        {"table": "b.two", "status": "clean"},
        {"table": "c.three", "status": "failed", "error": "y"},
    ]
    with pytest.raises(RuntimeError, match=r"2 table\(s\): a\.one: x; c\.three: y"):
        raise_if_failed(results)


def test_raise_if_failed_is_silent_without_failures():
    raise_if_failed([
        {"table": "a", "status": "clean"},
        {"table": "b", "status": "skipped"},
        {"table": "c", "status": "deduped"},
    ])
