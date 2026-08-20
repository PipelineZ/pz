-- output: lake.orders_curated (parquet, replace)
select
    o.id,
    o.amount,
    c.email
from staging.stg_orders as o
join staging.src_crm__customers as c
  on c.id = o.customer_id
