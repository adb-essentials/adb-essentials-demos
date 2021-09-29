-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Set-up Databricks SQL Endpoint
-- MAGIC 
-- MAGIC Follow instructions to [set-up Databricks SQL endpoint](https://docs.microsoft.com/en-us/azure/databricks/sql/admin/sql-endpoints#--create-a-sql-endpoint).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Run the SQL below in [Databricks SQL](/sql)

-- COMMAND ----------

SELECT 
    addr_state,
    -- Change application_type column to purpose to see cache in action
    application_type,
    sum(loan_amnt) total_loan_amnt,
    avg(loan_amnt) avg_loan_amnt
FROM delta_adb_essentials.loans_silver_partitioned
-- Change date range to 2015-12-01 to see cache in action
WHERE loan_issue_date > "2016-12-01"
GROUP BY 1, 2
ORDER BY total_loan_amnt DESC

-- COMMAND ----------


