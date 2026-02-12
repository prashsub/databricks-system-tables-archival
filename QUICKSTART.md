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

# Trigger a manual run
databricks bundle run system_tables_archival_workflow

# Full refresh (clears streaming checkpoints -- safe, never deletes archive data)
databricks pipelines start-update <pipeline-id> --full-refresh

# Check pipeline status
databricks pipelines get <pipeline-id>

# Check job run history
databricks jobs list-runs --job-id <job-id> --limit 5
```

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

## Troubleshooting

```bash
# Stale checkpoint (> 7 days behind or "Delta sharing table null" error)
databricks pipelines start-update <pipeline-id> --full-refresh

# View pipeline event log
databricks pipelines list-updates <pipeline-id>

# Destroy and redeploy (dev only)
databricks bundle destroy
databricks bundle deploy
```
