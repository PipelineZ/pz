INSERT INTO {{ sink('lake', 'totals_b', strategy: 'replace', format: 'parquet', path: 'out_b/') }}
select customer, count(*) as orders, sum(amount) as total
from {{ source('files', 'orders_b') }}
group by customer
