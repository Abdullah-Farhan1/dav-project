# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup (run once)
# MAGIC Creates the schema and the empty Delta tables for Seismic Pulse:
# MAGIC bronze, silver, quarantine, execution log and schema-drift log.
# MAGIC Safe to re-run: every statement uses IF NOT EXISTS.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "seismic_pulse"
VOLUME_PATH = "/Volumes/workspace/default/seismic"   # where the raw files live

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw copy of every event revision
# MAGIC All USGS fields are kept as STRING on purpose, so a type change in the source can never crash ingestion.
# MAGIC Casting happens in Silver. Primary key: (event_id, updated_ms).

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_earthquakes (
  event_id         STRING,
  mag              STRING,
  place            STRING,
  time_ms          STRING,
  updated_ms       STRING,
  tz               STRING,
  url              STRING,
  detail           STRING,
  felt             STRING,
  cdi              STRING,
  mmi              STRING,
  alert            STRING,
  status           STRING,
  tsunami          STRING,
  sig              STRING,
  net              STRING,
  code             STRING,
  ids              STRING,
  sources          STRING,
  types            STRING,
  nst              STRING,
  dmin             STRING,
  rms              STRING,
  gap              STRING,
  mag_type         STRING,
  event_type       STRING,
  title            STRING,
  longitude        STRING,
  latitude         STRING,
  depth            STRING,
  extra_properties MAP<STRING, STRING>,
  source_file      STRING,
  batch_id         STRING,
  load_type        STRING,
  load_timestamp   TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: cleaned, typed, one row per event (latest revision)
# MAGIC Primary key: event_id.

# COMMAND ----------

SILVER_DDL = """
(
  event_id        STRING,
  event_time      TIMESTAMP,
  event_date      DATE,
  event_hour      INT,
  updated_time    TIMESTAMP,
  magnitude       DOUBLE,
  mag_type        STRING,
  latitude        DOUBLE,
  longitude       DOUBLE,
  depth_km        DOUBLE,
  depth_category  STRING,
  place           STRING,
  region          STRING,
  review_status   STRING,
  significance    INT,
  felt            INT,
  cdi             DOUBLE,
  mmi             DOUBLE,
  alert           STRING,
  tsunami         INT,
  net             STRING,
  event_type      STRING,
  nst             INT,
  gap             DOUBLE,
  rms             DOUBLE,
  dmin            DOUBLE,
  title           STRING,
  batch_id        STRING,
  load_timestamp  TIMESTAMP
) USING DELTA
"""
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_earthquakes {SILVER_DDL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine: Bronze rows that fail Silver validation (bad cast, missing id or time)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_quarantine (
  event_id         STRING,
  updated_ms       STRING,
  reject_reason    STRING,
  raw_record       STRING,
  batch_id         STRING,
  load_timestamp   TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operational tables: execution log and schema-drift log

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS pipeline_execution_logs (
  run_id           STRING,
  layer            STRING,
  load_type        STRING,
  parameter        STRING,
  start_time       TIMESTAMP,
  end_time         TIMESTAMP,
  status           STRING,
  rows_inserted    BIGINT,
  rows_updated     BIGINT,
  rows_quarantined BIGINT,
  error_message    STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS schema_drift_log (
  detected_at      TIMESTAMP,
  batch_id         STRING,
  source_file      STRING,
  drift_type       STRING,
  column_name      STRING,
  detail           STRING
) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: tables exist and raw files are visible

# COMMAND ----------

import os

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

monthly = sorted(os.listdir(f"{VOLUME_PATH}/full_load_monthly"))
print("Monthly files in Volume:", len(monthly), "| first:", monthly[0], "| last:", monthly[-1])
print("Sample files:", [f for f in os.listdir(VOLUME_PATH) if f.endswith(".geojson")])
