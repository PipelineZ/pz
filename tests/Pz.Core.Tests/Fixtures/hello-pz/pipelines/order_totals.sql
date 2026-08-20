INSERT INTO {{ sink('lake', 'order_totals', format: 'csv', path: 'totals/', strategy: 'replace') }}
select customer_id, sum(amount) as total
from {{ ref('stg_orders') }}
group by customer_id
