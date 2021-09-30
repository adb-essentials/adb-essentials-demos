# Databricks notebook source
# MAGIC %md
# MAGIC # Batch inference
# MAGIC 
# MAGIC **Note:** This notebook does not run out of the box, but requires a dataset and registerd ML model, as described below.

# COMMAND ----------

# Clean up
spark.sql(f"DROP TABLE IF EXISTS wine.withPredictions")
dbutils.fs.rm("mnt/delta/inference_results", recurse=True)
spark.sql(f"CREATE DATABASE IF NOT EXISTS wine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch model inference
# MAGIC 
# MAGIC In this example we use a ML model from the model registry to classify new data in a table row by row. We classify a new batch of new wines from the [wine quality dataset](https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/), stored in the Delta table *NewWineData* in the *wine* database. The *wine_quality* model was trained in an earlier step and saved to the Model Registry.

# COMMAND ----------

# DBTITLE 1,New data to classify
# Read new data from Delta table
new_data = spark.table("wine.NewWineData")
display(new_data)

# COMMAND ----------

# DBTITLE 1,Load ML model to classify new data
import mlflow.pyfunc

# Fetch latest procuction model
model_name = 'wine_quality'
model = mlflow.pyfunc.load_model(f"models:/{model_name}/production")
model

# COMMAND ----------

# DBTITLE 1,Batch inference
# Load model into a Spark UDF, so it can be applied to the Delta table row by row
apply_model_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{model_name}/production")

# Apply model to the new data
with_predictions = new_data.withColumn("prediction",apply_model_udf('fixed_acidity', 'volatile_acidity', 'citric_acid', 'residual_sugar', 'chlorides', 'free_sulfur_dioxide', 'total_sulfur_dioxide', 'density', 'pH', 'sulphates', 'alcohol', 'is_red'))
# Each row now has an associated prediction.
display(with_predictions)

# COMMAND ----------

# DBTITLE 1,Save as new Delta table
# Save dataframe to Delta format
delta_folder = "/mnt/delta/inference_results"
with_predictions.write.format("delta").save(delta_folder)

# Register Delta table to database
spark.sql(f"CREATE TABLE wine.withPredictions USING DELTA LOCATION '{delta_folder}'")

# COMMAND ----------

# MAGIC %md
# MAGIC As next step the code can be automized using **scheduled jobs**.
