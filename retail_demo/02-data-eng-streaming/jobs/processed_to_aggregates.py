# Databricks notebook source
dbutils.widgets.text('database', 'adb_tpch_retail', 'Database')

# COMMAND ----------

database = dbutils.widgets.get("database")
print(f'Database param set as: {database}')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${database}.orders_region_mktsegment as
# MAGIC select 
# MAGIC   year(ol_orderdate) order_year,
# MAGIC   r_name as region,
# MAGIC   n_name as nation,
# MAGIC   c_mktsegment as mkt_segment,
# MAGIC   count(distinct ol_custkey) as no_customers,
# MAGIC   count(distinct ol_orderkey) as no_orders,
# MAGIC   sum(ol_grossprice) as total_price,
# MAGIC   avg(ol_grossprice) as avg_price,
# MAGIC   min(ol_grossprice) as min_price,
# MAGIC   max(ol_grossprice) as max_price,
# MAGIC   sum(ol_quantity) as total_qty
# MAGIC from ${database}.orders_lineitem_fct as o
# MAGIC left join ${database}.customer_stg as c
# MAGIC on o.ol_custkey = c.c_custkey
# MAGIC left join ${database}.nation_stg as n
# MAGIC on c.c_nationkey = n.n_nationkey
# MAGIC left join ${database}.region_stg as r
# MAGIC on n.n_regionkey = r.r_regionkey
# MAGIC group by 1,2,3,4
