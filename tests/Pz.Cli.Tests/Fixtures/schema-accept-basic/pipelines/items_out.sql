INSERT INTO {{ sink('lake', 'items_out', strategy: 'replace', format: 'parquet', path: 'items/') }}
select * from {{ source('pg', 'items') }}
