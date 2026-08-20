INSERT INTO {{ sink('mart', 'mart.orders_current', strategy: 'merge', keys: ['order_id']) }}
select
    order_id,
    customer_id,
    amount,
    status,
    updated_at
from {{ source('erp', 'dbo.orders', partition_column: 'order_id', partitions: 4, retry: { max_attempts: 3 }) }}
where updated_at > {{ watermark('erp', 'dbo.orders') }}
