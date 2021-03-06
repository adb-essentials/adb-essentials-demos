# Databricks notebook source
# MAGIC %md
# MAGIC # Read and write data
# MAGIC 
# MAGIC This notebook show how to read and data with Databricks and write it into the Delta format.
# MAGIC 
# MAGIC We will mostly use Python and SQL in this demo. It is possible to switch between languages (Python, R, Scala, SQL), filesystem commands or markdown language using magic commands (*%\<command\>*) at the beginning of a cell.
# MAGIC 
# MAGIC *Created with Databricks runtime 10.4 LTS.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data
# MAGIC 
# MAGIC Load CSV file from bike sharing [sample dataset](https://docs.microsoft.com/en-us/azure/databricks/data/databricks-datasets).

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/bikeSharing/data-001

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark dataframe

# COMMAND ----------

# Load to Spark dataframe
path = '/databricks-datasets/bikeSharing/data-001/'

sparkDF = spark.read.format('csv').options(header='true', inferSchema='true').load(path+'day.csv')
display(sparkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC You can **plot** Spark dataframes in the notebook output or see **table statistics** in the Data Profile.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pandas dataframe

# COMMAND ----------

# Load to Pandas dataframe
# Note that we have to add '/dbfs' to the path when reading from DBFS with Python
import pandas as pd

pandasDF = pd.read_csv('/dbfs'+path+'day.csv')
pandasDF

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pandas-on-Spark dataframe
# MAGIC 
# MAGIC Pandas is a popular Python library for data manipulation and analysis and works well for smaller datasets. However, Pandas runs on a single machine and is not scalable for big data. The [Pandas Spark API](https://docs.microsoft.com/en-us/azure/databricks/languages/pandas-spark) allows to distribute the execution on a Spark cluster while using the Pandas API. It is available on clusters that run Databricks Runtime 10.0 and above. Use [Koalas](https://docs.microsoft.com/en-us/azure/databricks/languages/koalas) for older runtimes.

# COMMAND ----------

# Load to pandas-on-Spark dataframe (requires DBR 10.0 or above)
import pyspark.pandas as ps

spark_pandasDF = ps.read_csv(path+'day.csv')
display(spark_pandasDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Save as Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC Saving data as a [table](https://docs.microsoft.com/en-us/azure/databricks/data/tables) will make it discoverable for other users in the data menu to the left, where they are grouped within databases. Table metadata will be stored in a metastore and point to the underlying files, which and abstracts away the storace location when working with data. Creating a table will also allow to query the data using SQL. Here we will save tables in the [Delta format](https://docs.microsoft.com/en-us/azure/databricks/delta/).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a database in which the table will be saved
# MAGIC CREATE DATABASE IF NOT EXISTS bike_sharing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed tables

# COMMAND ----------

# Load raw data into Spark dataframe
bike_sharing_day = spark.read.format('csv').options(header='true', inferSchema='true').load(path+'day.csv')

# Save the Spark dataframe we created earlier as a managed table
bike_sharing_day.write.saveAsTable("bike_sharing.day")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- It is now possible to query the table with SQL
# MAGIC select * from bike_sharing.day

# COMMAND ----------

# MAGIC %md
# MAGIC Tables in Databricks can be [managed or unmanaged](https://docs.microsoft.com/en-us/azure/databricks/data/tables#--managed-and-unmanaged-tables). 
# MAGIC 
# MAGIC **Managed tables** <br>
# MAGIC A managed table is a table for which Databricks manages both the data and the metadata. Deleting a table using the *DROP TABLE* command deletes both the metadata and data. The default location of managed data is either
# MAGIC - In the Databricks File Storage (DBFS) connected to the workspace, 
# MAGIC - Or in a managed cloud storage location when using [Unity Catalog](https://docs.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/), which is specified when setting au a catalog.
# MAGIC 
# MAGIC **Unmanaged tables** <br>
# MAGIC Another option is to let Databricks manage the metadata, while you control the data location. We refer to this as an unmanaged table. Databricks manages the relevant metadata, so when using *DROP TABLE*, only the metadata is removed and the table is no longer visible in the workspace. The data is still present in the path you provided.
# MAGIC 
# MAGIC **Note:** DBFS is not intended for production data, we recommend saving data in your own object storage, such as Azure Data Lake Storage Gen2 (ADLS2). Unmanaged tables with the data stored in cloud storage are therefore recommended for classical workspaces. With Unity Catalog the standard are managed tables, which store the data in a specified cloud storage location.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmanaged tables
# MAGIC 
# MAGIC The underlying data for unmanaged tables are typically saved in external storage locations. 
# MAGIC 
# MAGIC **The code below will only work if you set up a connection to an external storage location!** See notebook "0-setup".

# COMMAND ----------

# Load raw data into Spark dataframe
bike_sharing_hour = spark.read.format('csv').options(header='true', inferSchema='true').load(path+'hour.csv')

# COMMAND ----------

# Save as unmanaged Delta table
# Insert your storage location below: 'abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>'
adls_path = 'abfss://adb-essentials@thorstenstorage.dfs.core.windows.net/bike_sharing' 
table_name = 'bike_sharing.hour'

# Write data to external location
bike_sharing_hour.write.format('delta').save(adls_path)

# Link table in the metastore
spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{adls_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query table with SQL
# MAGIC select * from bike_sharing.hour

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC 
# MAGIC Delete the created databases and files. Read more on the recommended way to delete tables or replace the table content [here](https://docs.microsoft.com/en-us/azure/databricks/data/tables#--delete-a-table). 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment before running
# MAGIC -- DROP TABLE IF EXISTS bike_sharing.day;
# MAGIC -- DROP TABLE IF EXISTS bike_sharing.hour;
# MAGIC -- DROP DATABASE IF EXISTS bike_sharing;

# COMMAND ----------

# dbutils.fs.rm(adls_path, recurse=True)
