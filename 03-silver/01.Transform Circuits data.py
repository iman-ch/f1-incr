# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Circuits Data
# MAGIC 1. Read bronze `circuits` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`circuitId` $\rightarrow$ `circuit_id`, `circuitName` $\rightarrow$ `circuit_name`)
# MAGIC 4. Rename columns to make them more meaningful (`lat` $\rightarrow$ `latitude`, `long` $\rightarrow$ `longitude`)
# MAGIC 5. Filter out rows where `circuit_id` is null (business key validation)
# MAGIC 6. Remove duplicate records
# MAGIC 7. Transform values of columns `circuit_name` and `locality` to Title Case
# MAGIC 8. Write the transformed data to silver `circuits` table

# COMMAND ----------

# MAGIC %md
# MAGIC Below changes are required to implement Incremental Load Processing
# MAGIC 1. Accept batch_id as a parameter to the notebook
# MAGIC 2. Process data for only the batch_id being passed in (i.e., filter reading from bronze using the batch_id)
# MAGIC 3. Add created_timestamp, updated_timestamp and batch_id to the silver table. 
# MAGIC 4. Merge the processed data to the silver table
# MAGIC     -  created_timestamp should only be populated at the time of inserting/ creating the record. It should not be updated during the merge update
# MAGIC     - Ensure that we are not overwriting the data in silver table by older bronze data (re-run scenario)

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.circuits'
silver_table = f'{catalog_name}.{silver_schema}.circuits'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_df = (
    spark.table(bronze_table).filter((F.col("batch_id")== v_batch_id ))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC spark.read.table -> additional read options

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2

# COMMAND ----------

# DBTITLE 1,Cell 8
circuits_selected_df = circuits_df.select(
    F.col("circuitId"),
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"),
    F.col("ingestion_timestamp"),
    F.col("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3, 4

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
        .withColumnsRenamed({
            "circuitId": "circuit_id",
            "circuitName": "circuit_name",
            "lat": "latitude",
            "long": "longitude"
        })
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
)

# COMMAND ----------

# circuits_valid_df = circuits_renamed_df.filter(
#    F.col("circuit_id").isNotNull()
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7

# COMMAND ----------

# DBTITLE 1,Step 7
circuits_final_df = (
    circuits_distinct_df
        .withColumn('circuit_name', F.initcap(F.col("circuit_name")))
        .withColumn('locality', F.initcap(F.col("locality")))
        .withColumn("batch_id", F.lit(v_batch_id))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8

# COMMAND ----------

write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_id = s.circuit_id",
    columns_to_update=[
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingestion_timestamp",
        "source_file",
        "batch_id"        
    ]
)

# COMMAND ----------

display(spark.table(silver_table))