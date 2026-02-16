# Quick Start

Commands-only reference. For detailed explanations, see [README.md](README.md) and [docs/architecture/](docs/architecture/).

## Prerequisites

- Databricks CLI >= 0.281.0
- Unity Catalog enabled workspace
- System tables enabled (account-level)
- `CREATE CATALOG` / `CREATE SCHEMA` permissions

## First-Time Setup

```bash
# 1. Authenticate
databricks auth login --host https://<workspace>.cloud.databricks.com --profile <profile>

# 2. Validate the bundle
databricks bundle validate

# 3. Deploy
databricks bundle deploy

# 4. Create catalog and schemas (one-time)
databricks bundle run system_tables_setup

# 5. Run the full workflow to verify
databricks bundle run system_tables_archival_workflow
```

## Day-to-Day Operations

```bash
# Deploy changes after editing
databricks bundle deploy

# Trigger a manual run (streaming → dedup → batch)
databricks bundle run system_tables_archival_workflow

# Full refresh (clears streaming checkpoints -- safe, dedup removes duplicates automatically)
databricks pipelines start-update <pipeline-id> --full-refresh

# Check pipeline status
databricks pipelines get <pipeline-id>

# Check job run history
databricks jobs list-runs --job-id <job-id> --limit 5
```

## Workflow Tasks

The archival workflow runs 3 tasks sequentially:

| Task | Purpose | Typical Runtime |
|------|---------|----------------|
| `streaming_pipeline` | Incremental streaming ingest (27 tables) | ~2 min |
| `dedup_streaming_sinks` | Check for and remove duplicates. Skips clean tables. | ~5 min (clean) |
| `batch_companion` | Watermark MERGE (4 tables) + full overwrite (6 tables) | ~4 min |

## Excluding Tables

Set `exclude_tables` in `databricks.yml` under the target:

```yaml
targets:
  dev:
    variables:
      exclude_tables: "system.marketplace.listing_funnel_events,system.marketplace.listing_access_events"
```

Then deploy normally. Excluded tables stop refreshing but archive data is retained.

For single-table exclusion via CLI:

```bash
databricks bundle deploy --var="exclude_tables=system.marketplace.listing_funnel_events"
```

> **Note:** The `--var` CLI flag splits on commas. For multi-table exclusion, set `exclude_tables` in `databricks.yml` instead.

## Multi-Environment

```bash
# Dev (default)
databricks bundle deploy
databricks bundle run system_tables_archival_workflow

# Prod
databricks bundle deploy --target prod
databricks bundle run system_tables_archival_workflow --target prod
```

## Table Optimizations

All archive tables are configured with:
- **CLUSTER BY AUTO** — automatic liquid clustering (enforced on every dedup run)
- **Predictive Optimization** — enabled at schema level (auto OPTIMIZE, VACUUM, ZORDER)

These are set during initial setup and maintained by the dedup notebook on every run.

## Troubleshooting

```bash
# Stale checkpoint (> 7 days behind or "Delta sharing table null" error)
# Safe: dedup task will automatically clean up duplicates from the re-append
databricks pipelines start-update <pipeline-id> --full-refresh

# View pipeline event log
databricks pipelines list-updates <pipeline-id>

# Destroy and redeploy (dev only)
databricks bundle destroy
databricks bundle deploy
```
