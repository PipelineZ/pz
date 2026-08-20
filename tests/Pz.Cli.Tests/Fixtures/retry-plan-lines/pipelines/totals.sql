INSERT INTO [{{ sink('lake', 'totals', strategy: 'replace', retry: { max_attempts: 4, base_delay: '1s', max_delay: '90s' }, format: 'parquet', path: 'out/totals/') }}, {{ sink('lake', 'raw', strategy: 'replace', format: 'csv', path: 'out/raw/') }}]
select customer, count(*) as orders, sum(amount) as total
from {{ source('files', 'orders') }}
group by customer
