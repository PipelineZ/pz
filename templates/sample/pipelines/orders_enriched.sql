INSERT INTO {{ sink('lake', 'orders_curated', strategy: 'replace', format: 'parquet') }}
select
    o.id,
    o.amount,
    c.email
from {{ ref('stg_orders') }} as o
join {{ source('raw', 'customers') }} as c
  on c.id = o.customer_id
