INSERT INTO {{ sink('lake', 'totals', strategy: 'append', duplicates: 'accept', format: 'parquet', path: 'out/') }}
select customer, count(*) as orders, sum(amount) as total
from {{ source('files', 'orders') }}
group by customer
