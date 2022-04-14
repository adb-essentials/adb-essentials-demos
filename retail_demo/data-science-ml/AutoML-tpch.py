# Databricks notebook source
# MAGIC %md # AutoML
# MAGIC 
# MAGIC [Databricks AutoML](https://docs.databricks.com/applications/machine-learning/automl.html) helps you automatically build machine learning models both through a UI and programmatically. It prepares the dataset for model training and then performs and records a set of trials (using HyperOpt), creating, tuning, and evaluating multiple models. 
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you will:<br>
# MAGIC  - Load data from merged delta table
# MAGIC  - Run AutoML in Python
# MAGIC  - Interpret the results of an AutoML run

# COMMAND ----------

# MAGIC %md 
# MAGIC **Setup**
# MAGIC 
# MAGIC For this notebook you have to use **Databricks Runtime 8.3 ML or later**
# MAGIC **Data Source** This notebook uses the data that is automatically deployed in your DBFS hence you don't need to download / upload any data. DBFS is the Databricks File System.
# MAGIC 
# MAGIC Please note that using DBFS is not a best practice and you should not store any data on DBFS. For demo purpuses and reproducability of the training we do however use the data that is automatically availalbe for development on DBFS.

# COMMAND ----------

# Remove data from previous runs
dbutils.fs.rm("/adbessentials/returndata_small", recurse=True)
spark.sql("DROP TABLE IF EXISTS powerbi_tpch_dltdemo.returndata_small")

# COMMAND ----------

# DBTITLE 1,Look into database
# MAGIC %sql
# MAGIC show tables in powerbi_tpch_dltdemo

# COMMAND ----------

# DBTITLE 1,Use database for entire notebook
# MAGIC %sql use powerbi_tpch_dltdemo

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from databricks import automl

# COMMAND ----------

# DBTITLE 1,Read the data from the database
df = spark.table("lineitem_landing")
display(df)

# COMMAND ----------

# DBTITLE 1,Create a new dataset for ML
#specify features
columns = ["l_quantity","l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct", "l_shipmode"]

#create new dataset with the columns specified; if you're on the community edition comment out the cell and limit by 10000
returndata_complete = df.select(*(columns))

#using community edition - limit rows
returndata_small = returndata_complete.orderBy(rand()).limit(10000)

display(returndata_small)


# COMMAND ----------

# DBTITLE 1,Save data
#create and add table to directoy
deltalocation = "/adbessentials/returndata_small"
dbutils.fs.mkdirs(deltalocation)

returndata_small.write.save(deltalocation)
#returndata_small.write.mode("overwrite").save(deltalocation) #use this if table exists

# Register as table in metastore database
spark.sql("CREATE TABLE returndata_small USING DELTA LOCATION '/adbessentials/returndata_small'")
display(returndata_small)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train the model using Databricks AutoML. 
# MAGIC AutoML automatically trains models and finds the best hyperparameters. Additionally it collects all the trainings and displays it through notebooks so that you can view, edit and run the source code for each model. It can be used for classification, regression and forecasting and trains models based on scikit-learn, xgboost and LightGBM packages. More information can be found on the [docs](https://docs.microsoft.com/en-us/azure/databricks/applications/machine-learning/automl). For time series forecasting you need to use Databricks Runtime 10.0 ML or above.

# COMMAND ----------

ml = automl.classify(returndata_small, target_col="l_returnflag", timeout_minutes=30)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we trained the models, we can look into the results of each run and also take a closer look at the data exploration notebook. All parts are mapped in the results of the previous cell. 
