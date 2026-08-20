INSERT INTO {{ sink('lake', 'orders_out', strategy: 'replace', format: 'parquet', path: 'orders/') }}
select * from {{ source('pg', 'orders') }}
