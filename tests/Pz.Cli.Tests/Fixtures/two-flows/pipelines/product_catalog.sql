INSERT INTO {{ sink('lake', 'products_out', strategy: 'replace', format: 'csv', path: 'out/products/') }}
select id, name
from {{ source('raw', 'products') }}
