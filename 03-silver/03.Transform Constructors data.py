# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Constructors Data
# MAGIC 1. Read bronze `constructors` table
# MAGIC 2. Keep only the columns required for analytics (Drop `url` column)
# MAGIC 3. Standardise column names using snake_case (`constructorId` → `constructor_id`)
# MAGIC 4. Rename columns to make them more meaningful (`name` → `constructor_name`)
# MAGIC 5. Remove duplicate records
# MAGIC 6. Transform values of column `nationality` to Title Case
# MAGIC 7. Write the transformed data to silver `constructors` table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")  

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.constructors'
silver_table = f'{catalog_name}.{silver_schema}.constructors'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructors_df = (
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
constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3, 4

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
        .withColumnsRenamed({
            "constructorId": "constructor_id",
            "name": "constructor_name"
        })
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
        .withColumn('nationality', F.initcap(F.col("nationality")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8

# COMMAND ----------

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))