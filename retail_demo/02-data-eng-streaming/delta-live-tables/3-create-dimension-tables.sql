-- Databricks notebook source
create live table part_dim(
  constraint valid_partkey expect (p_partkey is not null)
)
comment "The part dimension table."
as select *
from live.part_landing;

-- COMMAND ----------

-- join customer, region and nation tables as Power BI recommends star schema over snowflake schema

-- COMMAND ----------

create live table customer_nation_region_dim as 
select *
from live.customer_landing c
left join live.nation_landing n
on c.c_nationkey = n.n_nationkey
left join live.region_landing r
on n.n_regionkey = r.r_regionkey

-- COMMAND ----------

-- create date dimension table based on min and max date from orders landing table

-- COMMAND ----------

create live table date_dim(
)
comment "Date dimension table"
as with date_range as (
  select
  explode(
  sequence(
  (select min(o_orderdate) from live.orders_landing), 
  (select max(o_orderdate) from live.orders_landing), 
  interval 1 day)
  ) as date
)

select
  cast(date_format(date,"yyyyMMdd") as int) as date_key,
  date, 
  cast(date_format(date,"yyyy") as int) as year,
  cast(date_format(date,"yyyyMM") as int) as year_month
from date_range
