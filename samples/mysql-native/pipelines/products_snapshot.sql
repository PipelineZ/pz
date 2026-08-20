INSERT INTO {{ sink('mart', 'products_snapshot', strategy: 'replace') }}
select
    product_id,
    name,
    category,
    unit_price
from {{ source('shop', 'products') }}
