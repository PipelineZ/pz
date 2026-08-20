INSERT INTO {{ sink('mart', 'mart.events_log', strategy: 'append', duplicates: 'accept') }}
select
    id,
    event_type,
    payload,
    occurred_at
from {{ source('erp', 'dbo.events', partition_column: 'id', partitions: 4, retry: { max_attempts: 3 }) }}
where id >  coalesce({{ watermark('erp', 'dbo.events') }}, cast(0 as bigint))                              -- initial: 0
  and id <= least(coalesce({{ watermark('erp', 'dbo.events') }}, cast(0 as bigint)) + 500, cast(2000 as bigint))  -- max_window: 500, until: 2000
