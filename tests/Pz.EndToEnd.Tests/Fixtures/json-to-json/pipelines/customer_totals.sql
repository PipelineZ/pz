INSERT INTO {{ sink('lake', 'customer_totals', strategy: 'replace', format: 'json', path: 'out/') }}
select customer, count(*) as orders, sum(amount) as total
from {{ source('files', 'events') }}
group by customer
