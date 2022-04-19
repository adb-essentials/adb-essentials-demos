# Databricks notebook source
dbutils.widgets.text('database', 'adb_tpch_retail', 'Database')

# COMMAND ----------

database = dbutils.widgets.get("database")
print(f'Database param set as: {database}')

# COMMAND ----------

orders_path = "dbfs:/databricks-datasets/tpch/delta-001/orders/"
orders_df = spark.read.format("delta").load(orders_path)
orders_df.write.mode("overwrite").saveAsTable(f"{database}.orders_stg")

lineitem_path = "dbfs:/databricks-datasets/tpch/delta-001/lineitem/"
lineitem_df = spark.read.format("delta").load(lineitem_path)
lineitem_df.write.mode("overwrite").saveAsTable(f"{database}.lineitem_stg")

# COMMAND ----------


