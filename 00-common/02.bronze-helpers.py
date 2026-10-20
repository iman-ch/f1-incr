# Databricks notebook source
# helper function to add file metadata for ingestion 
from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return (
        df
        .withColumn('ingestion_timestamp', F.current_timestamp())
        .withColumn('source_file', F.col('_metadata.file_path'))
    )
                    


# COMMAND ----------

def write_to_bronze (
    input_df,
    target_table,
    batch_id
):
    final_df = input_df.withColumn("batch_id", F.lit(v_batch_id))
    (
        final_df
            .write
            .format('delta')
            .mode('overwrite')
            .option('overwriteSchema', True)
            .partitionBy('batch_id')
            .option('replaceWhere', f"batch_id = '{v_batch_id}'")
            .saveAsTable(table_name)
    )