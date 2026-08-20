INSERT INTO {{ sink('lake', 'orders_curated', format: 'parquet', strategy: 'replace') }}
select
    o.id,
    o.amount,
    c.email
from {{ ref('stg_orders') }} as o
join {{ source('crm', 'customers', path: 'data/customers.csv', format: 'csv') }} as c
  on c.id = o.customer_id
