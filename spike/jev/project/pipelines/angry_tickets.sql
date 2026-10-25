INSERT INTO {{ sink('lake', 'angry_tickets', strategy: 'replace', format: 'csv') }}
select t.id, t.subject, jev_prob(t, 'the customer is angry') as p_angry
from {{ source('raw', 'tickets') }} t
where jev(t, 'the customer is angry')
