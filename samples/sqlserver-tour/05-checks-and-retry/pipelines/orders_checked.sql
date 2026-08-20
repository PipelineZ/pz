INSERT INTO {{ sink('mart', 'mart.orders_checked', strategy: 'replace') }}
select
    order_id,
    customer_id,
    amount,
    status,
    updated_at
from {{ source('erp', 'dbo.orders') }}
