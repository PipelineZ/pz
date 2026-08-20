INSERT INTO {{ sink('mart', 'mart.customers_dim', strategy: 'replace') }}
select
    customer_id,
    name,
    upper(country) as country_code,
    created_at
from {{ source('erp', 'dbo.customers') }}
