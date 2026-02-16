# Databricks notebook source
# MAGIC %md
# MAGIC # System Tables Archive — One-Time Setup
# MAGIC
# MAGIC Creates the target catalog and all schemas required by the streaming pipeline
# MAGIC and batch companion notebook. Run this **once** before the first pipeline execution.

# COMMAND ----------

dbutils.widgets.text("target_catalog", "system_tables_archive", "Target Catalog")

# COMMAND ----------

TARGET_CATALOG = dbutils.widgets.get("target_catalog")

print(f"Setting up catalog: {TARGET_CATALOG}")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
print(f"  Catalog '{TARGET_CATALOG}' ready.")

# COMMAND ----------

SCHEMAS = [
    "access",
    "billing",
    "compute",
    "data_classification",
    "data_quality_monitoring",
    "lakeflow",
    "marketplace",
    "mlflow",
    "query",
    "serving",
    "sharing",
    "storage",
]

for schema in SCHEMAS:
    fqn = f"{TARGET_CATALOG}.{schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
    spark.sql(f"ALTER SCHEMA {fqn} ENABLE PREDICTIVE OPTIMIZATION")
    print(f"  Schema '{fqn}' ready (predictive optimization enabled).")

print(f"\nSetup complete — {len(SCHEMAS)} schemas in '{TARGET_CATALOG}'.")
