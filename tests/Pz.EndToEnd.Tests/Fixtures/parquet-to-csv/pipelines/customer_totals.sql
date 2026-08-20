INSERT INTO {{ sink('lake', 'customer_totals', strategy: 'replace', format: 'csv', path: 'out/') }}
select customer, count(*) as orders, sum(amount) as total
from {{ source('files', 'orders') }}
group by customer
order by customer
