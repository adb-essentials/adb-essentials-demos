# Databricks notebook source
## Create Landing Tables

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

lineitem_path = "dbfs:/databricks-datasets/tpch/delta-001/lineitem/"

@dlt.table(
  comment="The raw lineitem dataset, ingested from /databricks-datasets."
)
def lineitem_landing():
  return (spark.read.format("delta").load(lineitem_path))

# COMMAND ----------

orders_path = "dbfs:/databricks-datasets/tpch/delta-001/orders/"

@dlt.table(
  comment="The raw orders dataset, ingested from /databricks-datasets."
)
def orders_landing():
  return (spark.read.format("delta").load(orders_path))

# COMMAND ----------

part_path = "dbfs:/databricks-datasets/tpch/delta-001/part/"

@dlt.table(
  comment="The raw part dataset, ingested from /databricks-datasets."
)
def part_landing():
  return (spark.read.format("delta").load(part_path))

# COMMAND ----------

nation_path = "dbfs:/databricks-datasets/tpch/delta-001/nation/"

@dlt.table(
  comment="The raw nation dataset, ingested from /databricks-datasets."
)
def nation_landing():
  return (spark.read.format("delta").load(nation_path))

# COMMAND ----------

region_path = "dbfs:/databricks-datasets/tpch/delta-001/region/"

@dlt.table(
  comment="The raw region dataset, ingested from /databricks-datasets."
)
def region_landing():
  return (spark.read.format("delta").load(region_path))

# COMMAND ----------

customer_path = "dbfs:/databricks-datasets/tpch/delta-001/customer/"

@dlt.table(
  comment="The raw customer dataset, ingested from /databricks-datasets."
)
def customer_landing():
  return (spark.read.format("delta").load(customer_path))
