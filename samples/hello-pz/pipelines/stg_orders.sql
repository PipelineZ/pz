select
    id,
    customer_id,
    amount,
    status
from {{ source('crm', 'orders') }}
where amount >= {{ var('min_amount') }}
