INSERT INTO {{ sink('lake', 'orders_out', strategy: 'replace', format: 'csv', path: 'out/orders/') }}
select id, amount
from {{ source('raw', 'orders') }}
