# Databricks notebook source
# MAGIC %sql
# MAGIC show tables in powerbi_tpch;

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(o_orderkey), max(o_orderkey)
# MAGIC from adb_tpch_retail_dlt.orders_landing;

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(l_orderkey), max(l_orderkey)
# MAGIC from adb_tpch_retail_dlt.lineitem_landing;

# COMMAND ----------

(20220101 * 100000000) + 1

# COMMAND ----------

(20220101 * 100000000) + 30000000

# COMMAND ----------

# MAGIC %sql
# MAGIC select l_orderkey
# MAGIC from powerbi_tpch.lineitem
# MAGIC order by l_orderkey asc
# MAGIC limit 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct l_orderkey)
# MAGIC from adb_tpch_retail_dlt.lineitem_landing

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct o_orderkey)
# MAGIC from adb_tpch_retail_dlt.orders_landing

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe adb_tpch_retail_dlt.orders_landing;

# COMMAND ----------

orders_path = "dbfs:/databricks-datasets/tpch/delta-001/orders/"
orders_df = spark.read.format("delta").load(orders_path)
display(orders_df)
# orders_df.write.mode("overwrite").saveAsTable(f"{database}.orders_stg")

# lineitem_path = "dbfs:/databricks-datasets/tpch/delta-001/lineitem/"
# lineitem_df = spark.read.format("delta").load(lineitem_path)
# lineitem_df.write.mode("overwrite").saveAsTable(f"{database}.lineitem_stg")

# COMMAND ----------

lineitem_path = "dbfs:/databricks-datasets/tpch/delta-001/lineitem/"
lineitem_df = spark.read.format("delta").load(lineitem_path)
display(lineitem_df)

# COMMAND ----------

from pyspark.sql.functions import *

def generate_daily_orders_df(orders_df, date_str):
#   date_str = "20220101"
  daily_orders_df = (orders_df
         .withColumn("o_orderkey", (int(date_str) * 100000000) + orders_df.o_orderkey)
         .withColumn("o_orderdate", to_date(lit(date_str) ,"yyyyMMdd") )
        )
  return daily_orders_df

def generate_daily_lineitem_df(lineitem_df, date_str):
#   date_str = "20220101"
  daily_lineitem_df = (lineitem_df
         .withColumn("l_orderkey", (int(date_str) * 100000000) + lineitem_df.l_orderkey)
        )
  return daily_lineitem_df

# COMMAND ----------



# COMMAND ----------

date_str = "20220131"
new_df = generate_daily_orders_df(orders_df, date_str)
display(new_df)

# COMMAND ----------

new_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/orders/{date_str}/")

# COMMAND ----------

date_str = "20220131"
new_li_df = generate_daily_lineitem_df(lineitem_df, date_str)
display(new_li_df)

# COMMAND ----------

from datetime import date, timedelta

start = date(2022, 1, 1)
end = date(2022, 1, 31)
delta = timedelta(days=1)

gendate = start
while gendate < end:
    date_str = "{date.year}{date.month:02}{date.day:02}".format(date=gendate)
    print(f"generate data for {date_str}")

    new_orders_df = generate_daily_orders_df(orders_df, date_str)
    new_orders_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/orders/{date_str}/")
    
    new_lineitem_df = generate_daily_lineitem_df(lineitem_df, date_str)
    new_lineitem_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/lineitem/{date_str}/")
    
    gendate += delta

# COMMAND ----------

from datetime import date, timedelta

start = date(2022, 1, 31)
end = date(2022, 2, 3)
delta = timedelta(days=1)

gendate = start
while gendate < end:
    date_str = "{date.year}{date.month:02}{date.day:02}".format(date=gendate)
    print(f"generate data for {date_str}")

    new_orders_df = generate_daily_orders_df(orders_df, date_str)
    new_orders_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/orders/{date_str}/")
    
    new_lineitem_df = generate_daily_lineitem_df(lineitem_df, date_str)
    new_lineitem_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/lineitem/{date_str}/")
    
    gendate += delta

# COMMAND ----------

from datetime import date, timedelta

start = date(2022, 2, 3)
end = date(2022, 2, 6)
delta = timedelta(days=1)

gendate = start
while gendate < end:
    date_str = "{date.year}{date.month:02}{date.day:02}".format(date=gendate)
    print(f"generate data for {date_str}")

    new_orders_df = generate_daily_orders_df(orders_df, date_str)
    new_orders_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/orders/{date_str}/")
    
    new_lineitem_df = generate_daily_lineitem_df(lineitem_df, date_str)
    new_lineitem_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/lineitem/{date_str}/")
    
    gendate += delta

# COMMAND ----------

from datetime import date, timedelta

start = date(2022, 1, 1)
end = date(2022, 1, 4)
delta = timedelta(days=1)

gendate = start
while gendate < end:
    date_str = "{date.year}{date.month:02}{date.day:02}".format(date=gendate)
    print(f"generate data for {date_str} start")

    new_orders_df = generate_daily_orders_df(orders_df, date_str)
    new_orders_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/streaming/orders/{date_str}/")
    
    new_lineitem_df = generate_daily_lineitem_df(lineitem_df, date_str)
    new_lineitem_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/streaming/lineitem/{date_str}/")
    
    print(f"generate data for {date_str} completed")
    gendate += delta

# COMMAND ----------

from datetime import date, timedelta

start = date(2022, 1, 4)
end = date(2022, 1, 5)
delta = timedelta(days=1)

gendate = start
while gendate < end:
    date_str = "{date.year}{date.month:02}{date.day:02}".format(date=gendate)
    print(f"generate data for {date_str} start")

    new_orders_df = generate_daily_orders_df(orders_df, date_str)
    new_orders_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/streaming/orders/{date_str}/")
    
    new_lineitem_df = generate_daily_lineitem_df(lineitem_df, date_str)
    new_lineitem_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/streaming/lineitem/{date_str}/")
    
    print(f"generate data for {date_str} completed")
    gendate += delta

# COMMAND ----------

from datetime import date, timedelta

start = date(2022, 1, 5)
end = date(2022, 1, 6)
delta = timedelta(days=1)

gendate = start
while gendate < end:
    date_str = "{date.year}{date.month:02}{date.day:02}".format(date=gendate)
    print(f"generate data for {date_str} start")

    new_orders_df = generate_daily_orders_df(orders_df, date_str)
    new_orders_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/streaming/orders/{date_str}/")
    
    new_lineitem_df = generate_daily_lineitem_df(lineitem_df, date_str)
    new_lineitem_df.write.mode("overwrite").json(f"dbfs:/tmp/tahirfayyaz/streaming/lineitem/{date_str}/")
    
    print(f"generate data for {date_str} completed")
    gendate += delta

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/tmp/tahirfayyaz")

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/tmp/tahirfayyaz/orders/{date_str}")

# COMMAND ----------


