INSERT INTO {{ sink('lake', 'totals_a', strategy: 'replace', format: 'parquet', path: 'out_a/') }}
select customer, count(*) as orders, sum(amount) as total
from {{ source('files', 'orders_a') }}
group by customer
