INSERT INTO {{ sink('mart', 'orders_log', strategy: 'append', duplicates: 'accept') }}
select
    order_id,
    customer_id,
    amount,
    status,
    updated_at
from {{ source('shop', 'orders') }}
where updated_at > {{ watermark('shop', 'orders') }}
