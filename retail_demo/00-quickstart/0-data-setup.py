# Databricks notebook source
# MAGIC %md
# MAGIC # Setup the Lakehouse Platform on Azure 
# MAGIC 
# MAGIC **Note that the notebook will run if you have used the one-click deployment [here](https://github.com/adb-essentials/adb-essentials-demos/blob/main/README.md).**
# MAGIC 
# MAGIC <img src="https://github.com/adb-essentials/adb-essentials-demos/raw/main/.adb/ADB_Lakehouse_Platform.png" alt="azure lakehouse" width="500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Set the data location and type
# MAGIC 
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC 
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

storage_account_name = "adbessentialsstorage"
storage_container_name = "adb-demos"
secret_scope="essentials_secret_scope"
secret_key="storageKey"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: View the sample data
# MAGIC 
# MAGIC Load CSV file from bike sharing [sample dataset](https://docs.microsoft.com/en-us/azure/databricks/data/databricks-datasets).

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/bikeSharing/data-001

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Set the data location and type
# MAGIC 
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC 
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

file_location = f"wasbs://adb-demos@{storage_account_name}.blob.core.windows.net/"
file_type = "csv"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  dbutils.secrets.get(scope=f"{secret_scope}", key=f"{secret_key}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: (Read & Write) Copy the data 
# MAGIC 
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC 
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").load("/databricks-datasets/bikeSharing/data-001")
df.write.format(file_type).save(file_location+'/bikeSharing')
