-- The read declared where it is used: this source() carries the options `connections.yml` would
-- otherwise hold under `entities: products: read:`. One surface or the other, never both (PZ0341).
INSERT INTO {{ sink('lake', 'product_catalog', strategy: 'replace', format: 'csv') }}
select
    id,
    name,
    price
from {{ source('raw', 'products', path: 'data/products.csv', format: 'csv', columns: { id: 'bigint', name: 'varchar', price: 'double' }) }}
