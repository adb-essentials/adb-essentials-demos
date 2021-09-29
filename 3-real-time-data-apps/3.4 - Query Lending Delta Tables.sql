-- Databricks notebook source
SHOW TABLES IN delta_adb_essentials_dlt;

-- COMMAND ----------

USE delta_adb_essentials_dlt;

-- COMMAND ----------

SELECT *
FROM lendingclub_payments_gold

-- COMMAND ----------

SELECT *
FROM lendingclub_payments_gold_window

-- COMMAND ----------

DESCRIBE loans_gold_incr_agg_partitioned

-- COMMAND ----------

DESCRIBE DETAIL loans_gold_incr_agg_partitioned

-- COMMAND ----------

DESCRIBE HISTORY loans_gold_incr_agg_partitioned

-- COMMAND ----------

DESCRIBE HISTORY lendingclub_payments_gold_incr_agg

-- COMMAND ----------

SELECT *
FROM loans_gold_incr_agg_partitioned

-- COMMAND ----------

SELECT eventdate, COUNT(*)
FROM loans_gold_incr_agg_partitioned
GROUP BY eventdate
ORDER BY eventdate

-- COMMAND ----------


