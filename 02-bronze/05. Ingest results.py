# Databricks notebook source
# MAGIC %md
# MAGIC read csv

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC read JSON file using dataframe reader API

# COMMAND ----------

# DBTITLE 1,Define results schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

results_schema = StructType([
    StructField('date', DateType()),
    StructField('raceName', StringType()),
    StructField('round', IntegerType()),
    StructField('season', IntegerType()),
    StructField('url', StringType()),
    StructField('constructorId', StringType()),
    StructField('driverId', StringType()),
    StructField('grid', IntegerType()),
    StructField('laps', IntegerType()),
    StructField('number', IntegerType()),
    StructField('points', IntegerType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('status', StringType())
])

# COMMAND ----------

# read data from drivers file
results_df = (
    spark.read
        .format('json')
        .schema(results_schema)
        .option('node', 'FAILFAST')
        .load(source_file)
)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC metadata cols

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC write to bronze delta table

# COMMAND ----------

# DBTITLE 1,Drop existing results table
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
print(f"Dropped {table_name} — ready for fresh write")

# COMMAND ----------

write_to_bronze (
    input_df = results_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)