INSERT INTO {{ sink('mart', 'mart.order_totals', strategy: 'replace') }}
select
    customer_id,
    country,
    order_count,
    total_amount
from {{ source('erp', 'order_totals') }}
