INSERT INTO {{ sink('mart', 'mart.orders_from_proc', strategy: 'merge', keys: ['order_id']) }}
select
    order_id,
    customer_id,
    amount,
    status,
    updated_at
from {{ source('erp', 'recent_orders') }}
