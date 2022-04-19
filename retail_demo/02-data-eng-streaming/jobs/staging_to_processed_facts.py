# Databricks notebook source
dbutils.widgets.text('database', 'adb_tpch_retail', 'Database')

# COMMAND ----------

database = dbutils.widgets.get("database")
print(f'Database param set as: {database}')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${database}.orders_lineitem_fct
# MAGIC as select 
# MAGIC   o_orderkey as ol_orderkey,
# MAGIC   o_custkey as ol_custkey,
# MAGIC   o_orderstatus as ol_orderstatus,
# MAGIC   -- o_totalprice,
# MAGIC   o_orderdate as ol_orderdate,
# MAGIC   --  powerbi needs date as yyyymmdd int
# MAGIC   cast(date_format(o_orderdate,"yyyyMMdd") as int) as ol_orderdatekey,
# MAGIC   o_orderpriority as ol_orderpriority,
# MAGIC   o_clerk as ol_clerk,
# MAGIC   o_shippriority as ol_shippriority,
# MAGIC   o_comment as ol_comment,
# MAGIC   l_partkey as ol_partkey,
# MAGIC   l_suppkey as ol_suppkey,
# MAGIC   l_linenumber as ol_linenumber,
# MAGIC   l_quantity as ol_quantity,
# MAGIC   l_extendedprice as ol_extendedprice,
# MAGIC   l_discount as ol_discount,
# MAGIC   l_extendedprice - (l_extendedprice * l_discount) as ol_netprice,
# MAGIC   l_tax as ol_tax,
# MAGIC   (l_extendedprice - (l_extendedprice * l_discount)) * (1 + l_tax) as ol_grossprice,
# MAGIC   l_returnflag as ol_returnflag,
# MAGIC   l_linestatus as ol_linestatus,
# MAGIC   l_shipdate as ol_shipdate,
# MAGIC   l_commitdate as ol_commitdate,
# MAGIC   l_receiptdate as ol_receiptdate,
# MAGIC   l_shipinstruct as ol_shipinstruct,
# MAGIC   l_shipmode as ol_shipmode,
# MAGIC   l_comment as ol_line_comment
# MAGIC from ${database}.orders_stg o
# MAGIC left join ${database}.lineitem_stg l
# MAGIC on o.o_orderkey = l.l_orderkey
