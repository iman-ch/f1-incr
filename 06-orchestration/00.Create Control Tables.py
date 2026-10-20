# Databricks notebook source
# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %sql
# MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{control_schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC spark.sql(f"""
# MAGIC           CREATE TABLE IF NOT EXISTS {catalog_name}.{control_schema}.batch_control
# MAGIC             (
# MAGIC                 batch_id STRING,
# MAGIC                 status STRING,
# MAGIC                 created_timestamp TIMESTAMP,
# MAGIC                 updated_timestamp TIMESTAMP
# MAGIC             )
# MAGIC           """)