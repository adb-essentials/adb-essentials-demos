# Databricks notebook source
dbutils.widgets.text('database', 'adb_tpch_retail', 'Database')

# COMMAND ----------

database = dbutils.widgets.get("database")
print(f'Database param set as: {database}')

# COMMAND ----------

customer_path = "dbfs:/databricks-datasets/tpch/delta-001/customer/"
customer_df = spark.read.format("delta").load(customer_path)
customer_df.write.mode("overwrite").saveAsTable(f"{database}.customer_stg")

nation_path = "dbfs:/databricks-datasets/tpch/delta-001/nation/"
nation_df = spark.read.format("delta").load(nation_path)
nation_df.write.mode("overwrite").saveAsTable(f"{database}.nation_stg")

part_path = "dbfs:/databricks-datasets/tpch/delta-001/part/"
part_df = spark.read.format("delta").load(part_path)
part_df.write.mode("overwrite").saveAsTable(f"{database}.part_stg")

partsupp_path = "dbfs:/databricks-datasets/tpch/delta-001/partsupp/"
partsupp_df = spark.read.format("delta").load(partsupp_path)
partsupp_df.write.mode("overwrite").saveAsTable(f"{database}.partsupp_stg")

region_path = "dbfs:/databricks-datasets/tpch/delta-001/region/"
region_df = spark.read.format("delta").load(region_path)
region_df.write.mode("overwrite").saveAsTable(f"{database}.region_stg")

supplier_path = "dbfs:/databricks-datasets/tpch/delta-001/supplier/"
supplier_df = spark.read.format("delta").load(supplier_path)
supplier_df.write.mode("overwrite").saveAsTable(f"{database}.supplier_stg")

# COMMAND ----------


