-- Databricks notebook source
-- Create Fact Tables

-- COMMAND ----------

-- Join Orders and Line Items

-- COMMAND ----------

create or refresh streaming live table orders_lineitems_fct(
)
comment "Orders and line items joined as fact table"
tblproperties(pipelines.autoOptimize.zOrderCols = "ol_orderdatekey,l_partkey")
as select 
  o_orderkey as ol_orderkey,
  o_custkey as ol_custkey,
  o_orderstatus as ol_orderstatus,
  -- o_totalprice,
  o_orderdate as ol_orderdate,
  --  powerbi needs date as yyyymmdd int
  cast(date_format(o_orderdate,"yyyyMMdd") as int) as ol_orderdatekey,
  o_orderpriority as ol_orderpriority,
  o_clerk as ol_clerk,
  o_shippriority as ol_shippriority,
  o_comment as ol_comment,
  l_partkey as ol_partkey,
  l_suppkey as ol_suppkey,
  l_linenumber as ol_linenumber,
  l_quantity as ol_quantity,
  l_extendedprice as ol_extendedprice,
  l_discount as ol_discount,
  l_tax as ol_tax,
  l_returnflag as ol_returnflag,
  l_linestatus as ol_linestatus,
  l_shipdate as ol_shipdate,
  l_commitdate as ol_commitdate,
  l_receiptdate as ol_receiptdate,
  l_shipinstruct as ol_shipinstruct,
  l_shipmode as ol_shipmode,
  l_comment as ol_line_comment
from stream(live.orders_landing) o
inner join stream(live.lineitem_landing) l
on o.o_orderkey = l.l_orderkey
