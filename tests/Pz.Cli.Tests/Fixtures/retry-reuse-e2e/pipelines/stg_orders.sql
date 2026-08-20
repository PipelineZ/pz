INSERT INTO [{{ sink('ok', 'ok', strategy: 'append', duplicates: 'accept', format: 'parquet', path: 'out_ok/') }}, {{ sink('flaky', 'flaky', strategy: 'append', duplicates: 'accept', format: 'parquet', path: 'out_flaky/') }}]
select * from {{ source('crm', 'orders') }}
