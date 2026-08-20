-- Incremental extraction paired with an append sink is at-least-once: GitHub's `since` is
-- "at or after", so the boundary row re-lands on the next run, and a replayed run can re-deliver a
-- slice. pz refuses this pairing at compile time (PZ0214) unless you consent -- which a delta log
-- deliberately does. Dedup downstream by id, keeping the row with the highest updated_at.
INSERT INTO {{ sink('lake', 'issues_log', format: 'parquet', path: 'out/issues/', strategy: 'append', duplicates: 'accept') }}
select
    id,
    number,
    title,
    state,
    updated_at
from {{ source('github', 'issues') }}
