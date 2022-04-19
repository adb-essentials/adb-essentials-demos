# Databricks notebook source
dbutils.widgets.text('database', 'adb_tpch_retail', 'Database')

# COMMAND ----------

database = dbutils.widgets.get("database")
print(f'Database param set as: {database}')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ${database};
# MAGIC show tables in ${database};
