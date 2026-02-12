-- Freshness check for system tables archive.
-- Raises an error (failing the job task) if the archive has not been
-- updated in over 48 hours, providing time to remediate before the
-- 168-hour Delta Sharing VACUUM window causes data loss.

SELECT
  CASE
    WHEN DATEDIFF(HOUR, MAX(change_time), CURRENT_TIMESTAMP()) > 48
    THEN RAISE_ERROR(
      CONCAT('System tables archive is stale! Last update was ',
             CAST(DATEDIFF(HOUR, MAX(change_time), CURRENT_TIMESTAMP()) AS STRING),
             ' hours ago (threshold: 48h, VACUUM window: 168h).')
    )
    ELSE CONCAT('OK - last update ',
                CAST(DATEDIFF(HOUR, MAX(change_time), CURRENT_TIMESTAMP()) AS STRING),
                ' hours ago.')
  END AS freshness_status
FROM (
  SELECT MAX(last_modified) AS change_time
  FROM ${target_catalog}.information_schema.tables
  WHERE table_schema IN (
    'access', 'billing', 'compute', 'lakeflow', 'marketplace',
    'mlflow', 'serving', 'sharing', 'storage', 'query'
  )
)
