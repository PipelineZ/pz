select
    id,
    customer_id,
    amount,
    status
from staging.src_crm__orders
where amount >= 10
