-- Incremental read paired with a merge sink is effectively-once: the merge is keyed, so a replayed
-- run converges on the same rows rather than duplicating them. This is the pairing local files
-- cannot express -- see the incremental template for the at-least-once half.
INSERT INTO {{ sink('mart', 'mart.orders_current', strategy: 'merge', keys: ['order_id']) }}
select
    order_id,
    customer_id,
    amount,
    status,
    updated_at
from {{ source('erp', 'dbo.orders') }}
where updated_at > {{ watermark('erp', 'dbo.orders') }}
