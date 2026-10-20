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

source_file = f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC read JSON file using dataframe reader API

# COMMAND ----------

# DBTITLE 1,Define constructors schema
constructors_schema = "constructorId STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# read data from constructor file
constructors_df = (
    spark.read
        .format('json')
        .schema(constructors_schema)
        .option('node', 'FAILFAST')
        .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC metadata cols

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC write to bronze delta table

# COMMAND ----------

write_to_bronze (
    input_df = constructors_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))