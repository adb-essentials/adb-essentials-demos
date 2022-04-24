# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk load data into a Delta table with Auto Loader
# MAGIC 
# MAGIC The Databricks Auto Loader incrementally processes new data ariving in a cloud storage location. It can be used together with Spark structured streaming to detects and ingest files as soon as they arrive in cloud storage, or can be run periodically to only add new files.
# MAGIC 
# MAGIC Autoloader has two modes:
# MAGIC 
# MAGIC **Directory listing**: Allows you to quickly start Auto Loader streams without any permission configurations other than access to your data on cloud storage. Auto Loader automatically detects new files using lexical ordering, which reduces the amount of API calls it needs to make to detect new files.
# MAGIC 
# MAGIC **File notification**: Auto Loader automatically sets up a notification service and queue service in Azure, that subscribe to file events from the input directory. File notification mode is more performant and scalable for large input directories or a high volume of files but requires additional cloud permissions for set up. Use *.option("cloudFiles.useNotifications","true")*.
# MAGIC 
# MAGIC In this demo we are reading csv files containing population data from DBFS and ingest them as Delta files. You can create sample csv files with data shown in the [Auto Loader docs](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader). In production environments an external cloud storage is used instead.
# MAGIC 
# MAGIC *Created with Databricks runtime 10.4 LTS.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize

# COMMAND ----------

# Create file upload directory
user_dir = 'adb_essentials'
upload_path = "/FileStore/shared_uploads/" + user_dir + "/population_data_upload"

dbutils.fs.mkdirs(upload_path)

# Location for Delta files
write_path = '/tmp/delta/population_data'
checkpoint_path = '/tmp/delta/population_data/_checkpoints'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Auto Loader

# COMMAND ----------

# Set up the stream to begin reading incoming files from the upload_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'csv') \
  .option('header', 'true') \
  .schema('city string, year int, population long') \
  .load(upload_path)

# Start the stream.
# Use the checkpoint_path location to keep a record of all files that have already been uploaded to the upload_path location.
# For those that have been uploaded since the last check, write the newly-uploaded files' data to the write_path location.
df.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest files
# MAGIC 
# MAGIC You can now start to upload new csv files while you leave the stream from the cell above running. Use the "Upload Data" option in the notebook's file menu and select the target directory (*shared_uploads/adb_essentials/population_data_upload/*). 
# MAGIC 
# MAGIC Afer iploading a file, run the code below to see how Auto Loader has detected the new data and added it to the Delta table.

# COMMAND ----------

df_population = spark.read.format('delta').load(write_path)
display(df_population)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: COPY INTO
# MAGIC 
# MAGIC The SQL based [COPY INTO](https://docs.microsoft.com/en-us/azure/databricks/tutorials/copy-into) command is another way to load data incrementally into a Delta table. It is ideal when the source directory contains a small number of files.
# MAGIC 
# MAGIC Here are a few things to consider when choosing between Auto Loader and COPY INTO:
# MAGIC 
# MAGIC - If you’re going to ingest files in the order of thousands, you can use COPY INTO. If you are expecting files in the order of millions or more over time, use Auto Loader. Auto Loader can discover files more cheaply compared to COPY INTO and can split the processing into multiple batches.
# MAGIC - If your data schema is going to evolve frequently, Auto Loader provides better primitives around [schema inference and evolution](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/auto-loader#schema-inference).
# MAGIC - With Auto Loader, it’s harder to reprocess a select subset of files. Loading re-uploaded files can be a bit easier to manage with COPY INTO.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an empty, managed table
# MAGIC CREATE TABLE default.population (
# MAGIC   city STRING,
# MAGIC   year INT,
# MAGIC   population INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run the COPY INTO command after new files arrived in the storage location to incrementally add new data
# MAGIC COPY INTO default.population
# MAGIC FROM '/FileStore/shared_uploads/adb_essentials/population_data_upload'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC   'inferSchema' = 'true'
# MAGIC );
# MAGIC 
# MAGIC SELECT * FROM default.population;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up

# COMMAND ----------

dbutils.fs.rm(write_path, True)
dbutils.fs.rm(upload_path, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default.population;
