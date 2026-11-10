# Databricks notebook source
source_path = "/Volumes/guard_project/source/fraude_watchlist/source_data/"


# COMMAND ----------

dbutils.fs.ls(source_path)

# COMMAND ----------

input_stream = (spark.readStream
               .format("cloudFiles")
               .option("CloudFiles.format","json")
               .option("CloudFiles.schemaLocation","/Volumes/guard_project/source/fraude_watchlist/schema/")
               .option("CloudFiles.inferColumnTypes","true")
               .load(source_path)
)

# COMMAND ----------

from pyspark.sql import functions as F
transformed_df=input_stream.select(
    "*",
    F.col("_metadata.file_path").alias("file_path"),
    F.current_timestamp().alias("ingestion_timestamp")
)

# COMMAND ----------

streaming_query=(transformed_df.writeStream.format("delta")
.outputMode("Append")
.option("checkpointLocation", "/Volumes/guard_project/source/fraude_watchlist/checkpoint/")
.trigger(availableNow=True)
.toTable("guard_project.bronze.fraud_watchlist_batch_test")
)