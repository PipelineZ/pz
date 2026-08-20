-- output: lake.order_totals (csv, replace)
select customer_id, sum(amount) as total
from staging.stg_orders
group by customer_id
