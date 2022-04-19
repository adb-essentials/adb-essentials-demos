# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Delta Lake Tables for Loans Data to be used in Databricks SQL

# COMMAND ----------

db = "delta_adb_essentials"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# COMMAND ----------

# Configure location of loans stats parquet data
lspq_path = "/databricks-datasets/samples/lending_club/parquet/"
loan_stats = spark.read.parquet(lspq_path)

# COMMAND ----------

display(loan_stats)

# COMMAND ----------

loan_stats.write.format("delta").mode("append").saveAsTable(f"{db}.delta_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT addr_state, COUNT(*)
# MAGIC FROM delta_adb_essentials.delta_bronze
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_adb_essentials.loans_silver_partitioned
# MAGIC PARTITIONED BY (loan_issue_date)
# MAGIC AS SELECT
# MAGIC   TO_DATE(
# MAGIC     CAST(UNIX_TIMESTAMP(issue_d, 'MMM-yyyy') AS TIMESTAMP)
# MAGIC   ) loan_issue_date,
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_adb_essentials.delta_bronze
# MAGIC WHERE
# MAGIC   length(addr_state) = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE delta_adb_essentials.loans_silver_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta_adb_essentials.loans_silver_partitioned
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE delta_adb_essentials.loans_silver_partitioned
# MAGIC COMPUTE STATISTICS FOR COLUMNS addr_state, purpose, application_type, loan_amnt;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta_adb_essentials.loans_silver_partitioned
# MAGIC ZORDER BY (addr_state)

# COMMAND ----------


