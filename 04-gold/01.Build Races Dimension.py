# Databricks notebook source
# MAGIC %md
# MAGIC # Build Races Dimension
# MAGIC 1. Read silver `races` table
# MAGIC 2. Read silver `circuits` table
# MAGIC 3. Join data from `races` with `circuits`
# MAGIC 4. Select the required columns
# MAGIC     - races.season
# MAGIC     - races.round
# MAGIC     - races.race_name
# MAGIC     - races.race_date
# MAGIC     - circuits.circuit_name
# MAGIC     - circuits.locality
# MAGIC     - circuits.country
# MAGIC 6. write the transofrmed data to gold `dim_races` table
# MAGIC
# MAGIC 1. Accept batch_id as a parameter to the notebook
# MAGIC 2. Process data for only the batch_id being passed in (i.e., filter reading from silver using the batch_id)
# MAGIC 3. Add created_timestamp, updated_timestamp to the gold table. 
# MAGIC 4. Merge the processed data to the gold table
# MAGIC     - created_timestamp should only be populated at the time of inserting/ creating the record. It should not be updated during the merge update.

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - read source tables

# COMMAND ----------

circuits_df = (
    spark.table(f"{catalog_name}.{silver_schema}.circuits")
         .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------

races_df = (
    spark.table(f"{catalog_name}.{silver_schema}.races")
         .filter(F.col("batch_id") == v_batch_id)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - join races with circuits via circuit_id

# COMMAND ----------

dim_races_df = (
            races_df
                .join(
                    circuits_df,
                    races_df.circuit_id == circuits_df.circuit_id,
                    "inner"
                )
                .select (
                    races_df.season,
                    races_df.round,
                    races_df.race_name,
                    races_df.race_date,
                    circuits_df.circuit_name,
                    circuits_df.locality,
                    circuits_df.country
                )
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ###     Step 3 - write to gold dim_races table

# COMMAND ----------

(
    dim_races_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
)