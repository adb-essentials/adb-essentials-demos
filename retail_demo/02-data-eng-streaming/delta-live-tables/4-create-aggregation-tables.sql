-- Databricks notebook source
-- Create aggregation tables

-- COMMAND ----------

create live table orders_brand_aggregated (
  constraint valid_total_price expect (total_price > 0) on violation drop row
)
comment "Orders Brand Aggregated Table"
as select 
  a.ol_orderdatekey as ol_orderdatekey,
  a.ol_orderdate as orderdate, 
  b.p_brand as brand,
  b.p_type as type,
  sum(a.ol_extendedprice) total_price,
  sum(a.ol_quantity) total_quantity
from live.orders_lineitems_fct as a
inner join live.part_dim as b
on a.ol_partkey = b.p_partkey
group by 1, 2, 3, 4

-- COMMAND ----------

create live table orders_mfgr_aggregated (
  constraint valid_total_price expect (total_price > 0) on violation drop row
)
comment "Orders Mfgr Aggregated Table"
as select
  a.ol_orderdatekey as orderdatekey,
  a.ol_orderdate as orderdate,
  b.p_mfgr as mfgr, 
  sum(a.ol_extendedprice) total_price,
  sum(a.ol_quantity) total_quantity  
from live.orders_lineitems_fct as a
left outer join live.part_dim as b
on (a.ol_partkey = b.p_partkey)
group by orderdatekey, orderdate, mfgr
