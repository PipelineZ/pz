INSERT INTO {{ sink('lake', 'customer_totals', strategy: 'replace', format: 'parquet', path: 'out/') }}
select customer, count(*) as orders, sum(amount) as total
from {{ source('files', 'orders') }}
group by customer
