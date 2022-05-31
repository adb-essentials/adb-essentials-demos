-- Databricks notebook source
create or refresh streaming live table lineitem_landing 
comment "The raw lineitem dataset, ingested from /dbfs."
as select * from cloud_files("/tmp/tahirfayyaz/streaming/lineitem/", "json", map(
  "cloudFiles.schemaHints", """
        l_orderkey BIGINT, 
        l_partkey BIGINT,
        l_suppkey BIGINT,
        l_linenumber INT, 
        l_quantity FLOAT,
        l_extendedprice FLOAT,
        l_discount FLOAT, 
        l_tax FLOAT, 
        l_returnflag STRING,
        l_linestatus STRING,
        l_shipdate DATE,
        l_commitdate DATE,
        l_receiptdate DATE,
        l_shipinstruct STRING,
        l_shipmode STRING,
        l_comment STRING
      """
  ))

-- COMMAND ----------

create or refresh streaming live table orders_landing 
comment "The raw orders dataset, ingested from /tmp/tahirfayyaz using auto loader."
as select * from cloud_files("/tmp/tahirfayyaz/streaming/orders/", "json", map(
  "cloudFiles.schemaHints", """
        o_orderkey BIGINT, 
        o_custkey BIGINT,
        o_orderstatus STRING,
        o_totalprice FLOAT,
        o_orderdate DATE,
        o_orderpriority STRING,
        o_clerk STRING,
        o_shippriority INT,
        o_comment STRING
      """
  ))

-- COMMAND ----------

create or refresh live table part_landing
comment "The raw part dataset, ingested from /databricks-datasets."
as select * from delta.`/databricks-datasets/tpch/delta-001/part/`

-- COMMAND ----------

create or refresh live table nation_landing
comment "The raw nation dataset, ingested from /databricks-datasets."
as select * from delta.`/databricks-datasets/tpch/delta-001/nation/`

-- COMMAND ----------

create or refresh live table region_landing
comment "The raw region dataset, ingested from /databricks-datasets."
as select * from delta.`/databricks-datasets/tpch/delta-001/region/`

-- COMMAND ----------

create or refresh live table customer_landing
comment "The raw customer dataset, ingested from /databricks-datasets."
as select * from delta.`/databricks-datasets/tpch/delta-001/customer/`
