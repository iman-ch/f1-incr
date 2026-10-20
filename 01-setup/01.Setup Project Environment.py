# Databricks notebook source
# MAGIC %md
# MAGIC # Setup the porject environment for Formula1 Project
# MAGIC 1. Create External Location dbcourse-ext-dl01-formula1-incr
# MAGIC 2. Create Catalog formula1_incr
# MAGIC 3. Create Schemas landing, bronze, silver, gold
# MAGIC 4. Create Volume files in 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access Cloud Storage

# COMMAND ----------

# MAGIC %fs ls 'abfss://formula1-incr@dbcourseextdl01.dfs.core.windows.net/landing'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access External Location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS dbcourse_ext_dl01_formula1_incr
# MAGIC URL 'abfss://formula1-incr@dbcourseextdl01.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `dbcourse-sc`)
# MAGIC COMMENT 'External location for the formula1 container';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog formula1_incr

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS formula1_incr
# MAGIC    MANAGED LOCATION 'abfss://formula1-incr@dbcourseextdl01.dfs.core.windows.net'
# MAGIC    COMMENT 'formula1_incr project catalog';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas landing, bronze, silver, gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
# MAGIC     MANAGED LOCATION 'abfss://formula1-incr@dbcourseextdl01.dfs.core.windows.net/bronze';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
# MAGIC     MANAGED LOCATION 'abfss://formula1-incr@dbcourseextdl01.dfs.core.windows.net/silver';
# MAGIC CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
# MAGIC     MANAGED LOCATION 'abfss://formula1-incr@dbcourseextdl01.dfs.core.windows.net/gold';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG formula1_incr;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume Files

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME formula1_incr.landing.files
# MAGIC LOCATION 'abfss://formula1-incr@dbcourseextdl01.dfs.core.windows.net/landing';