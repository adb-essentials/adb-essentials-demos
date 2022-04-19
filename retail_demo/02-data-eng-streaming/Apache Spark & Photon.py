# Databricks notebook source
# MAGIC %md
# MAGIC ## Apache Spark & Photon
# MAGIC 
# MAGIC This notebook is design to show the difference in performance between a normal Apache Spark job and a Photon optimized job.
# MAGIC 
# MAGIC Clone this notebook and run one version using a DBR standard cluster and one version using a Photon DBR cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget & parameter for database name

# COMMAND ----------

dbutils.widgets.text('database', 'adb_tpch_retail', 'Database')

# COMMAND ----------

database = dbutils.widgets.get("database")
print(f'Database param set as: {database}')

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ${database};
# MAGIC show tables in ${database};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and write tpch dataset to Delta Lake

# COMMAND ----------

orders_path = "dbfs:/databricks-datasets/tpch/delta-001/orders/"
orders_df = spark.read.format("delta").load(orders_path)
orders_df.write.mode("overwrite").saveAsTable(f"{database}.orders_stg")

lineitem_path = "dbfs:/databricks-datasets/tpch/delta-001/lineitem/"
lineitem_df = spark.read.format("delta").load(lineitem_path)
lineitem_df.write.mode("overwrite").saveAsTable(f"{database}.lineitem_stg")

customer_path = "dbfs:/databricks-datasets/tpch/delta-001/customer/"
customer_df = spark.read.format("delta").load(customer_path)
customer_df.write.mode("overwrite").saveAsTable(f"{database}.customer_stg")

nation_path = "dbfs:/databricks-datasets/tpch/delta-001/nation/"
nation_df = spark.read.format("delta").load(nation_path)
nation_df.write.mode("overwrite").saveAsTable(f"{database}.nation_stg")

part_path = "dbfs:/databricks-datasets/tpch/delta-001/part/"
part_df = spark.read.format("delta").load(part_path)
part_df.write.mode("overwrite").saveAsTable(f"{database}.part_stg")

partsupp_path = "dbfs:/databricks-datasets/tpch/delta-001/partsupp/"
partsupp_df = spark.read.format("delta").load(partsupp_path)
partsupp_df.write.mode("overwrite").saveAsTable(f"{database}.partsupp_stg")

region_path = "dbfs:/databricks-datasets/tpch/delta-001/region/"
region_df = spark.read.format("delta").load(region_path)
region_df.write.mode("overwrite").saveAsTable(f"{database}.region_stg")

supplier_path = "dbfs:/databricks-datasets/tpch/delta-001/supplier/"
supplier_df = spark.read.format("delta").load(supplier_path)
supplier_df.write.mode("overwrite").saveAsTable(f"{database}.supplier_stg")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join orders & lineitem staging into fact table

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Aggregated Tables

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

# COMMAND ----------


