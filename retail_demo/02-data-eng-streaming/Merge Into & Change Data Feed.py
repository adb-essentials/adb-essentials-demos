# Databricks notebook source
# MAGIC %md
# MAGIC ## Merge Into & Change Data Feed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget & parameter for database name

# COMMAND ----------

dbutils.widgets.text('database', 'adb_tpch_retail', 'Database')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${database}.customer_stg

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(c_custkey) from ${database}.customer_stg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed on Customer Staging Table

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table ${database}.customer_stg
# MAGIC set tblproperties (delta.enableChangeDataCapture = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create demo customer CDC (change data capture) table

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table ${database}.customer_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${database}.customer_cdc_updates (
# MAGIC cdc_operation STRING, -- can be INSERT, UPDATE or MERGE
# MAGIC c_custkey BIGINT, 
# MAGIC c_name STRING,
# MAGIC c_address STRING, 
# MAGIC c_nationkey BIGINT, 
# MAGIC c_phone STRING, 
# MAGIC c_acctbal DECIMAL(18,2), 
# MAGIC c_mktsegment STRING, 
# MAGIC c_comment STRING) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert dummy Update and Insert values into the cdc table 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${database}.customer_cdc_updates   
# MAGIC VALUES ('UPDATE', 412445, 'Customer#000412445', '0QAB3OjYnbP6mA0B,kgf', 21, '31-421-403-4333', 5358.33, 'AUTOMOBILE', 'New comment');
# MAGIC INSERT INTO ${database}.customer_cdc_updates 
# MAGIC VALUES ('INSERT', 750001, 'Customer#000750001', 'BSD8Y9BR3dsandsa9dsa', 7, '31-442-406-4666', 1358.33, 'BUILDING', 'New customer');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${database}.customer_cdc_updates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge CDC change into customer stg table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ${database}.customer_stg as customer_stg
# MAGIC USING ${database}.customer_cdc_updates as customer_cdc_updates
# MAGIC ON customer_stg.c_custkey = customer_cdc_updates.c_custkey
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     c_custkey = customer_cdc_updates.c_custkey, 
# MAGIC     c_name = customer_cdc_updates.c_name,
# MAGIC     c_address = customer_cdc_updates.c_address, 
# MAGIC     c_nationkey = customer_cdc_updates.c_nationkey, 
# MAGIC     c_phone = customer_cdc_updates.c_phone, 
# MAGIC     c_acctbal = customer_cdc_updates.c_acctbal, 
# MAGIC     c_mktsegment = customer_cdc_updates.c_mktsegment, 
# MAGIC     c_comment = customer_cdc_updates.c_comment
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     c_custkey, 
# MAGIC     c_name,
# MAGIC     c_address, 
# MAGIC     c_nationkey, 
# MAGIC     c_phone, 
# MAGIC     c_acctbal, 
# MAGIC     c_mktsegment, 
# MAGIC     c_comment
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     customer_cdc_updates.c_custkey, 
# MAGIC     customer_cdc_updates.c_name,
# MAGIC     customer_cdc_updates.c_address, 
# MAGIC     customer_cdc_updates.c_nationkey, 
# MAGIC     customer_cdc_updates.c_phone, 
# MAGIC     customer_cdc_updates.c_acctbal, 
# MAGIC     customer_cdc_updates.c_mktsegment, 
# MAGIC     customer_cdc_updates.c_comment
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history ${database}.customer_stg;

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Table Changes using Change Data Feed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_changes('${database}.customer_stg', 3)

# COMMAND ----------


