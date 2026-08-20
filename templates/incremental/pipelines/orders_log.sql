-- The WHERE clause IS the incremental declaration: pz reads the stored watermark for this
-- entity, types the comparison from it, and bounds what the pipeline lands. On the first run
-- there is no watermark yet, so everything lands; after that, only rows newer than the highest
-- updated_at already delivered. Whether the bound also reaches extraction -- so the connector
-- itself reads less -- depends on the connector; local CSV has no predicate pushdown, so every
-- run still scans the whole file. See the sqlserver template for a connector that pushes the
-- bound into the query it sends.
--
-- Paired with an append sink this is at-least-once, not effectively-once: pz refuses that pairing
-- at compile time (PZ0214) unless you consent with `duplicates: 'accept'`, which a delta log
-- deliberately does. Effectively-once needs a sink that can merge, which no file-based connector
-- can -- see the sqlserver template for that half.
INSERT INTO {{ sink('lake', 'orders_log', format: 'parquet', strategy: 'append', duplicates: 'accept') }}
select
    order_id,
    customer_id,
    amount,
    status,
    updated_at
from {{ source('raw', 'orders') }}
where updated_at > {{ watermark('raw', 'orders') }}
