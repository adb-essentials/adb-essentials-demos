# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Loans Data as CSV files
# MAGIC 
# MAGIC Note: This might take a few minutes to run

# COMMAND ----------

df = spark.read.format('parquet').load("/databricks-datasets/samples/lending_club/parquet/")

# file_path = "/adbessentials/loan_stats_csv"
# file_path = "abfss://<container-name>@<account-name>.dfs.core.windows.net"
file_path = "abfss://data@dltdemostorage.dfs.core.windows.net/adbessentials/loan_stats_csv"

df.repartition(7200).write.mode('overwrite').format('csv').option("header", "true").save(file_path)

# COMMAND ----------

dbutils.fs.ls(file_path)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /adbessentials/loan_stats_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test reading the Loans CSV data

# COMMAND ----------

loans_df = spark.read.format('csv').option("header", "true").option("inferSchema","true").load("/adbessentials/loan_stats_csv")
display(loans_df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /adbessentials/
