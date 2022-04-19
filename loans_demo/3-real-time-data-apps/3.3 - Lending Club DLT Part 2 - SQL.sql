-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Incremental table

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE loans_events_silver(
  CONSTRAINT valid_timestamp EXPECT (timestamp > '2012-01-01'),
  CONSTRAINT valid_payment EXPECT (payment_amnt > 0)
)

COMMENT "A silver table for all payments"
AS SELECT *
FROM STREAM(LIVE.loans_events_bronze)
WHERE event_type = 'loan_payment'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Complete aggregated table

-- COMMAND ----------

CREATE LIVE TABLE loans_events_gold_complete(
  CONSTRAINT valid_total_payment EXPECT (total_payment > 0)
)
COMMENT "States with complete aggregation"
AS SELECT
  to_date(timestamp) as eventdate,
  addr_state,
  sum(funded_amnt) as total_funded,
  sum(payment_amnt) as total_payment,
  count(*) as total_payments
FROM LIVE.loans_events_silver
GROUP BY eventdate, addr_state

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Incremental aggregation table
-- MAGIC 
-- MAGIC [INCREMENTAL AGGREGATION tables](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-user-guide#--incremental-aggregation)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE loans_events_gold_incr_agg(
  CONSTRAINT valid_payment EXPECT (total_payment > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "States with incremental aggregation"
AS SELECT
  to_date(timestamp) as eventdate,
  addr_state,
  sum(funded_amnt) as total_funded,
  sum(payment_amnt) as total_payment,
  count(*) as total_payments
FROM STREAM(LIVE.loans_events_silver)
GROUP BY eventdate, addr_state

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Incremental aggregation with partitioned table

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE loans_events_gold_incr_agg_partitioned(
  CONSTRAINT valid_payment EXPECT (total_payment > 0) ON VIOLATION FAIL UPDATE
)
PARTITIONED BY ( eventdate )
COMMENT "partitioned date and states with incremental aggregation"
AS SELECT
  to_date(timestamp) as eventdate,
  addr_state,
  sum(funded_amnt) as total_funded,
  sum(payment_amnt) as total_payment,
  count(*) as total_payments
FROM STREAM(LIVE.loans_events_silver)
GROUP BY eventdate, addr_state

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Incremental windowed table
-- MAGIC 
-- MAGIC [`window` grouping expression](https://docs.databricks.com/sql/language-manual/functions/window.html)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE loans_events_gold_window
COMMENT "States with windowed agregation"
AS SELECT
  window as event_window,
  addr_state,
  sum(funded_amnt) as total_funded,
  sum(payment_amnt) as total_payment,
  count(*) as total_payments
FROM STREAM(LIVE.loans_events_silver)
GROUP BY window(timestamp, '10 MINUTES', '300 SECONDS'), addr_state;
