INSERT INTO {{ sink('lake', 'orders_out', strategy: 'append', duplicates: 'accept', format: 'parquet', path: 'out/') }}
select id, customer, amount
from {{ source('files', 'orders') }}
