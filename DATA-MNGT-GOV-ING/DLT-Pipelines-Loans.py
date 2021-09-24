# Databricks notebook source
# DBTITLE 1,Read CSV with Autoloader
import dlt
from pyspark.sql.types import *

sourceFile = "/adbessentials/loan_stats_csv"

@dlt.create_table(
  comment="The malware raw data",
  table_properties={
    "quality": "bronze"
  }
)
def loans_stats_stream_bronze():
  return spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation", "/adbessentials/loan_stats_csv_schema").load(sourceFile)

# COMMAND ----------

# DBTITLE 1,Filter Data 
from pyspark.sql.functions import col, length

@dlt.create_table(
  comment="remove bad records",
  table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("addr_state", "length(addr_state) = 2" )
def loans_stats_stream_silver():
  return dlt.read("loans_stats_stream_bronze").select("addr_state","loan_status")

# COMMAND ----------

# DBTITLE 1,Consolidate Data
@dlt.create_table(
  comment="Consolidation per state",
  table_properties={"quality": "gold"},
  spark_conf={"pipelines.trigger.interval": "1 hour"}
)
def loans_stats_stream_gold():
  return dlt.read("loans_stats_stream_silver").groupBy("addr_state").count()
