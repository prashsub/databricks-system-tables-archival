"""
System Tables Streaming Archive — SDP Pipeline Transformations

Incrementally copies all streaming-capable system tables into persistent
Delta sink tables using append_flow + Delta sinks.

Key properties:
- Full refresh is safe: re-appends to sinks, never deletes historical data.
- skipChangeCommits = true: handles upstream rolling deletes from Delta Sharing.
- Triggered mode: scheduled daily via Databricks Workflow.

Pipeline configuration parameter:
- target_catalog: Unity Catalog catalog for archived tables (e.g. system_tables_archive)
"""

from pyspark import pipelines as dp

TARGET_CATALOG = spark.conf.get("target_catalog", "system_tables_archive")

# ---------------------------------------------------------------------------
# Exclude Tables — parsed from comma-separated pipeline configuration.
# Accepts both "system.billing.usage" and "billing.usage" (auto-prefixed).
# ---------------------------------------------------------------------------
_raw_excludes = spark.conf.get("exclude_tables", "").strip()
EXCLUDE_TABLES = set()
if _raw_excludes:
    for _t in _raw_excludes.split(","):
        _t = _t.strip()
        if _t:
            EXCLUDE_TABLES.add(_t if _t.startswith("system.") else f"system.{_t}")

# ---------------------------------------------------------------------------
# Streaming Table Registry
# Verified against docs.databricks.com/aws/en/admin/system-tables and
# system.information_schema.tables.
# Only tables documented as streaming-capable are included here.
# ---------------------------------------------------------------------------

STREAMING_TABLES = [
    # access
    {"source": "system.access.audit",                    "schema": "access",      "table": "audit"},
    {"source": "system.access.column_lineage",           "schema": "access",      "table": "column_lineage"},
    {"source": "system.access.table_lineage",            "schema": "access",      "table": "table_lineage"},
    {"source": "system.access.clean_room_events",        "schema": "access",      "table": "clean_room_events"},
    {"source": "system.access.inbound_network",          "schema": "access",      "table": "inbound_network",   "delta_format": True},
    {"source": "system.access.outbound_network",         "schema": "access",      "table": "outbound_network"},
    # billing
    {"source": "system.billing.usage",                   "schema": "billing",     "table": "usage"},
    # compute
    {"source": "system.compute.clusters",                "schema": "compute",     "table": "clusters"},
    {"source": "system.compute.node_timeline",           "schema": "compute",     "table": "node_timeline"},
    {"source": "system.compute.warehouse_events",        "schema": "compute",     "table": "warehouse_events"},
    {"source": "system.compute.warehouses",              "schema": "compute",     "table": "warehouses"},
    # lakeflow
    {"source": "system.lakeflow.job_run_timeline",       "schema": "lakeflow",    "table": "job_run_timeline"},
    {"source": "system.lakeflow.job_task_run_timeline",  "schema": "lakeflow",    "table": "job_task_run_timeline"},
    {"source": "system.lakeflow.job_tasks",              "schema": "lakeflow",    "table": "job_tasks"},
    {"source": "system.lakeflow.jobs",                   "schema": "lakeflow",    "table": "jobs"},
    {"source": "system.lakeflow.pipelines",              "schema": "lakeflow",    "table": "pipelines"},
    {"source": "system.lakeflow.pipeline_update_timeline", "schema": "lakeflow",  "table": "pipeline_update_timeline", "delta_format": True},
    {"source": "system.lakeflow.zerobus_stream",         "schema": "lakeflow",    "table": "zerobus_stream",    "delta_format": True},
    {"source": "system.lakeflow.zerobus_ingest",         "schema": "lakeflow",    "table": "zerobus_ingest",    "delta_format": True},
    # marketplace
    {"source": "system.marketplace.listing_funnel_events",  "schema": "marketplace", "table": "listing_funnel_events"},
    {"source": "system.marketplace.listing_access_events",  "schema": "marketplace", "table": "listing_access_events"},
    # mlflow
    {"source": "system.mlflow.experiments_latest",       "schema": "mlflow",      "table": "experiments_latest"},
    {"source": "system.mlflow.runs_latest",              "schema": "mlflow",      "table": "runs_latest"},
    {"source": "system.mlflow.run_metrics_history",      "schema": "mlflow",      "table": "run_metrics_history"},
    # serving
    {"source": "system.serving.served_entities",         "schema": "serving",     "table": "served_entities"},
    {"source": "system.serving.endpoint_usage",          "schema": "serving",     "table": "endpoint_usage"},
    # sharing
    {"source": "system.sharing.materialization_history",  "schema": "sharing",    "table": "materialization_history"},
]

# ---------------------------------------------------------------------------
# Register Sinks & Flows
#
# Each streaming table gets:
#   1. A Delta sink (persistent archive table outside pipeline lifecycle)
#   2. An append_flow that reads incrementally with skipChangeCommits = true
# ---------------------------------------------------------------------------


def register_sink_and_flow(config: dict) -> None:
    """Register a Delta sink and append_flow for a single system table."""
    sink_name = f"{config['schema']}_{config['table']}_sink"
    flow_name = f"{config['schema']}_{config['table']}_flow"
    target_table = f"{TARGET_CATALOG}.{config['schema']}.{config['table']}"
    source_table = config["source"]
    needs_delta_format = config.get("delta_format", False)

    dp.create_sink(
        name=sink_name,
        format="delta",
        options={"tableName": target_table},
    )

    @dp.append_flow(name=flow_name, target=sink_name)
    def _flow(src=source_table, delta_fmt=needs_delta_format):
        reader = spark.readStream.option("skipChangeCommits", "true")
        if delta_fmt:
            reader = reader.option("responseFormat", "delta")
        return reader.table(src)


for _cfg in STREAMING_TABLES:
    if _cfg["source"] in EXCLUDE_TABLES:
        print(f"SKIPPING (excluded): {_cfg['source']}")
        continue
    register_sink_and_flow(_cfg)
