# Databricks notebook source
# MAGIC %md
# MAGIC # ML on Databricks: Load data prep for AutoML
# MAGIC 
# MAGIC **Note:** This notebook will not run our of the box, it requires the Delta table *adb_essentials.loan_stats_ml*, which is created in the previous session.

# COMMAND ----------

# DBTITLE 1,Initialise
# Load Delta table from storage location and save as table in metastore
#loan_data = spark.read.format("delta").load("/adbessentials/loan_stats")
#loan_data.write.format("delta").saveAsTable("adb_essentials.loan_stats")

# Remove data from previous runs
dbutils.fs.rm("/adbessentials/loan_stats_ml", recurse=True)
spark.sql("DROP TABLE IF EXISTS adb_essentials.loan_stats_ml")

# COMMAND ----------

# MAGIC %md
# MAGIC To run this notebook, you have to **use Databricks Runtime 7.6 ML or later**. <br/>
# MAGIC 
# MAGIC ### Source Data for this notebook
# MAGIC The data used is a modified version of the public data from [Lending Club](https://www.kaggle.com/wordsforthewise/lending-club). It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here].(https://resources.lendingclub.com/LCDataDictionary.xlsx).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Problem Statement: Classifying loan grade
# MAGIC 
# MAGIC We want to produce a ML model that can grade a loan based on a number of provided factors. The grade could then be used to define the intrest rate or whether or not a loan is given to a applicant.

# COMMAND ----------

# DBTITLE 1,Load data and data prep
from pyspark.sql.functions import *

# Read raw data from table
data = spark.table("adb_essentials.loan_stats")
display(data)

# COMMAND ----------

# Select the columns needed
features = ["loan_amnt", "term", "grade", "emp_length", "home_ownership", "annual_inc", "purpose", "zip_code", "dti", "delinq_2yrs", "total_bc_limit"]

# Create new dataset
loan_stats_full = data.select(*(features))

loan_stats_ml = loan_stats_full.orderBy(rand()).limit(10000) # Limit rows loaded to facilitate running on Community Edition
display(loan_stats_ml)

# COMMAND ----------

# DBTITLE 1,Save to new Delta table
# Create directory where table should be stored
delta_location = "/adbessentials/loan_stats_ml"
dbutils.fs.mkdirs(delta_location)

# Save table in Delta Lake format to cloud storage
# loan_stats_ml.write.save("/adbessentials/loan_stats_ml") # use this for creating a new table
loan_stats_ml.write.mode("overwrite").save(delta_location ) # use this if table already exists

# Register as table in metastore database
spark.sql("CREATE TABLE adb_essentials.loan_stats_ml USING DELTA LOCATION '/adbessentials/loan_stats_ml'")
display(loan_stats_ml)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Train model with AutoML
# MAGIC Use Databricks AutoML to train a ML classifier to 

# COMMAND ----------

from databricks import automl
summary = automl.classify(loan_stats_ml, target_col="grade", timeout_minutes=30)

# COMMAND ----------

# DBTITLE 1,Register model to model registry
import mlflow

# Find best model
model_uri = summary.best_trial.model_path

# Register to Model Registry
model_name = "thorsten_adb_essentials_automl"
registered_model_version = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC **Next steps:** Analyze AutoML results, do data prep to improve model.
