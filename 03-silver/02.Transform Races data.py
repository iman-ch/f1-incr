# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Races Data
# MAGIC 1. Read bronze `races` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`raceName` $\rightarrow$ `race_name`
# MAGIC 4. Rename columns to make them more meaningful (`date` $\rightarrow$ `race_date`)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of column `race_name` to Title Case
# MAGIC 7. Write the transformed data to silver `races` table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.races'
silver_table = f'{catalog_name}.{silver_schema}.races'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_df = (
    spark.table(bronze_table)
         .filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

# MAGIC %md
# MAGIC spark.read.table -> additional read options

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2

# COMMAND ----------

# DBTITLE 1,Cell 8
races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3, 4

# COMMAND ----------

races_renamed_df = (
    races_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "raceName": "race_name",
            "date": "race_date"
        })
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(["season","round"])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7

# COMMAND ----------

# DBTITLE 1,Step 7
races_final_df = (
    races_distinct_df
        .withColumn('race_name', F.initcap(F.col("race_name")))
        .withColumn("batch_id", F.lit(v_batch_id))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8

# COMMAND ----------

write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_id",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))