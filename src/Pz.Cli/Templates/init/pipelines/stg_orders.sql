select
    id,
    customer_id,
    amount,
    status
from {{ source('raw', 'orders') }}
where amount >= {{ var('min_amount') }}
