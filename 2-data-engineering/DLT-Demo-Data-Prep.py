# Databricks notebook source
df = spark.read.format('parquet').load("/databricks-datasets/samples/lending_club/parquet/")

df.repartition(7200).write.mode('overwrite').format('csv').option("header", "true").save("/adbessentials/loan_stats_csv")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /adbessentials/loan_stats_csv

# COMMAND ----------

cc = spark.read.format('csv').option("header", "true").option("inferSchema","true").load("/adbessentials/loan_stats_csv")
display(cc)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /adbessentials/
