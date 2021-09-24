# Databricks notebook source
# MAGIC %md
# MAGIC To run this notebook, you have to [create a cluster](https://docs.databricks.com/clusters/create.html) with version **Databricks Runtime 8.1 or later** and [attach this notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#attach-a-notebook-to-a-cluster) to that cluster. <br/>
# MAGIC 
# MAGIC ### Source Data for this notebook
# MAGIC The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wendykan/lending-club-loan-data). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).

# COMMAND ----------

db = "delta_adb_essentials"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# COMMAND ----------

# MAGIC %md ## Import Data and create pre-Delta Lake Table

# COMMAND ----------

# DBTITLE 0,Import Data and create pre-Databricks Delta Table
# Configure location of loans stats parquet data
lspq_path = "/databricks-datasets/samples/lending_club/parquet/"

# Load in a Spark Dataframe
loan_stats = spark.read.parquet(lspq_path)

# Consolidate loans number per state
loan_by_state = loan_stats.select("addr_state", "loan_status").groupBy("addr_state").count()

# Generate Temporory View
loan_by_state.createOrReplaceTempView("loan_by_state")

# Display loans by state
display(loan_by_state)

# COMMAND ----------

# MAGIC %md <img src="https://databricks.com/wp-content/uploads/2020/09/delta-lake-medallion-model-scaled.jpg" width=1012/>

# COMMAND ----------


# Configure Delta Lake Silver Path
DELTALAKE_SILVER_PATH = "/adbessentials/loan_by_state_delta"

# Remove folders
dbutils.fs.rm(DELTALAKE_SILVER_PATH, recurse=True)
dbutils.fs.rm("/adbessentials/loan_stats", recurse=True)

# COMMAND ----------

# MAGIC %md # ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Creating Delta Lake Table

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Option 1 : Create table and convert to Delta Lake format simultaneously

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Current example is creating a new table instead of in-place import so will need to change this code
# MAGIC DROP TABLE IF EXISTS loan_by_state_delta;
# MAGIC 
# MAGIC CREATE TABLE loan_by_state_delta
# MAGIC USING delta
# MAGIC LOCATION '/adbessentials/loan_by_state_delta'
# MAGIC AS SELECT * FROM loan_by_state where length(addr_state) = 2;
# MAGIC 
# MAGIC -- View Delta Lake table
# MAGIC SELECT * FROM loan_by_state_delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL loan_by_state_delta

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Option 2 : Convert to Delta Lake format, then create Table

# COMMAND ----------

loan_stats.write.format("delta").save("/adbessentials/loan_stats")

spark.sql("DROP TABLE IF EXISTS loan_stats")

spark.sql("CREATE TABLE loan_stats USING DELTA LOCATION '/adbessentials/loan_stats'")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table loan_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table delta.`/adbessentials/loan_stats`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loan_stats

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Option 3 : Create Table via Delta API 1.0 then ingest data

# COMMAND ----------

dbutils.fs.rm("/adbessentials/loan_by_state_delta_bis", recurse=True)

# COMMAND ----------

from delta import *
from pyspark.sql.types import *

_ = spark.sql('DROP TABLE IF EXISTS loan_by_state_delta_bis;')

# Create table in the metastore
loanByStateTable = DeltaTable.createIfNotExists(spark) \
    .tableName("loan_by_state_delta_bis") \
    .comment('created via delta api 1.0 ') \
    .location("/adbessentials/loan_by_state_delta_bis") \
    .addColumn('addr_state', StringType(),comment='2 digit state code') \
    .addColumn('count', IntegerType(),comment='count of loans per state') \
    .property('created.by.user','amine.benhamza@databricks.com') \
    .property('quality', 'silver') \
    .property('delta.autoOptimize.optimizeWrite','true') \
    .property('delta.autoOptimize.autoCompact','true') \
    .execute()

# COMMAND ----------

loan_by_state.write.option("overwriteSchema", "true").mode('overwrite').saveAsTable('loan_by_state_delta_bis')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM loan_by_state_delta_bis

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) What's under the hood

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /adbessentials/loan_stats

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /adbessentials/loan_stats/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support: `DELETE`, `UPDATE`, `MERGE INTO`
# MAGIC 
# MAGIC Delta Lake brings ACID transactions and full DML support to data lakes.
# MAGIC 
# MAGIC >Parquet does **not** support these commands - they are unique to Delta Lake.

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `DELETE` on the Delta Lake table
# MAGIC DELETE FROM loan_by_state_delta WHERE addr_state = 'IA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `loan_by_state_delta` Delta Lake table
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_delta group by addr_state

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY loan_by_state_delta

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /adbessentials/loan_by_state_delta/_delta_log

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) UPDATE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `UPDATE` on the Delta Lake table
# MAGIC UPDATE loan_by_state_delta SET `count` = 270000 WHERE addr_state = 'WA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `loan_by_state_delta` Delta Lake table
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_delta group by addr_state

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY loan_by_state_delta

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /adbessentials/loan_by_state_delta/_delta_log

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# Let's create a simple table to merge
items = [('IA', 10), ('CA', 2500), ('OR', None)]
cols = ['addr_state', 'count']
merge_table = spark.createDataFrame(items, cols)
merge_table.createOrReplaceTempView("merge_table")
display(merge_table)

# COMMAND ----------

# MAGIC %md Instead of writing separate `INSERT` and `UPDATE` statements, we can use a `MERGE` statement. 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO loan_by_state_delta as d
# MAGIC USING merge_table as m
# MAGIC on d.addr_state = m.addr_state
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review current loans within the `loan_by_state_delta` Delta Lake table
# MAGIC select addr_state, sum(`count`) as loans from loan_by_state_delta group by addr_state

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY loan_by_state_delta

# COMMAND ----------

# MAGIC %md #### Access data in a specific version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM loan_by_state_delta VERSION AS OF 1 order by addr_state

# COMMAND ----------

# MAGIC %md #### Rollback a table to a specific version using `RESTORE`

# COMMAND ----------

# MAGIC %sql 
# MAGIC RESTORE loan_by_state_delta VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY loan_by_state_delta
