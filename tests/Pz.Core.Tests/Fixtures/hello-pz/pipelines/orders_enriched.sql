INSERT INTO {{ sink('lake', 'orders_curated', format: 'parquet', path: 'curated/orders/', strategy: 'replace') }}
select
    o.id,
    o.amount,
    c.email
from {{ ref('stg_orders') }} as o
join {{ source('crm', 'customers') }} as c
  on c.id = o.customer_id
