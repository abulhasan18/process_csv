import duckdb

# Connect
con = duckdb.connect()

# Final query with corrected timestamp format
query = """
SELECT
  -- Essential Columns (18)
  strptime(CAST("{job_stats_createTime}" AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f')::DATE AS date,
  strptime(CAST("{job_stats_createTime}" AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f') AS start_time,
  strptime(CAST("{job_stats_endTime}" AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f') AS end_time,
  "{user_email}" AS user_email,
  "{project_id}" AS project_id,
  "{region}" AS region,
  "{job_id}" AS job_id,
  substr("{query_text}", 1, 500) AS query_text_short,
  "{query_text}" AS query_text_full,
  CASE
    WHEN "{job_status_state}" = 'DONE' AND "{job_status_error}" IS NULL THEN 'SUCCESS'
    ELSE 'FAILED'
  END AS status,
  NULL AS cache_hit,
  "{job_status_error}" AS error_message,
  TRY_CAST("{bytes_processed}" AS BIGINT) AS bytes_processed,
  TRY_CAST("{bytes_billed}" AS BIGINT) AS bytes_billed,
  ROUND((TRY_CAST("{bytes_billed}" AS DOUBLE) / POW(1024, 4)) * 5, 4) AS estimated_cost,
  datediff(
    'second',
    strptime(CAST("{job_stats_createTime}" AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f'),
    strptime(CAST("{job_stats_endTime}" AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f')
  ) AS duration_seconds,
  TRY_CAST("{slot_ms}" AS BIGINT) AS slot_ms,
  TRY_CAST("{output_rows}" AS BIGINT) AS output_rows,

  -- Derived Columns (7)
  ROUND(TRY_CAST("{bytes_processed}" AS DOUBLE) / POW(1024, 3), 2) AS gb_processed,
  ROUND(TRY_CAST("{bytes_billed}" AS DOUBLE) / POW(1024, 3), 2) AS gb_billed,
  ROUND(
    ((TRY_CAST("{bytes_billed}" AS DOUBLE) / POW(1024, 4)) * 5) / NULLIF(TRY_CAST("{output_rows}" AS DOUBLE), 0),
    6
  ) AS cost_per_row,
  ROUND(
    (TRY_CAST("{bytes_processed}" AS DOUBLE) / NULLIF(TRY_CAST("{output_rows}" AS DOUBLE), 0)) / POW(1024, 2),
    2
  ) AS mb_per_row,
  CASE
    WHEN TRY_CAST("{bytes_processed}" AS DOUBLE) > POW(1024, 3) * 5 AND TRY_CAST("{output_rows}" AS INT) < 1000 THEN TRUE
    ELSE FALSE
  END AS is_wasteful_query,
  strftime(strptime(CAST("{job_stats_createTime}" AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f'), '%A') AS day_of_week,
  CAST(strftime(strptime(CAST("{job_stats_createTime}" AS VARCHAR), '%Y-%m-%d %H:%M:%S.%f'), '%H') AS INT) AS hour

FROM read_csv_auto('bq-logs-aman.csv')
WHERE "{query_text}" IS NOT NULL
"""

# Replace placeholders with actual column names
colmap = {
    "user_email": "protopayload_auditlog__authenticationInfo__principalEmail",
    "project_id": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobName__projectId",
    "region": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobName__location",
    "job_id": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobName__jobId",
    "query_text": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobConfiguration__query__query",
    "job_status_state": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobStatus__state",
    "job_status_error": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobStatus__error",
    "job_stats_createTime": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobStatistics__createTime",
    "job_stats_endTime": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobStatistics__endTime",
    "bytes_processed": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobStatistics__totalProcessedBytes",
    "bytes_billed": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobStatistics__totalBilledBytes",
    "slot_ms": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobStatistics__totalSlotMs",
    "output_rows": "protopayload_auditlog__servicedata_v1_bigquery__jobCompletedEvent__job__jobStatistics__queryOutputRowCount"
}

# Replace placeholders in the query string
for placeholder, actual in colmap.items():
    query = query.replace(f'"{{{placeholder}}}"', f'"{actual}"')

# Execute and show first few rows
df = con.execute(query).fetchdf()
print(df.head())

df.to_csv("refined_bq_usage_utf8.csv", index=False, encoding="utf-8")

