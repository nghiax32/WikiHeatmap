-- One row per loaded event, renamed and typed. No dedupe here: fact_edits
-- dedupes only the rows it is about to (re)build.
select
    event_id,
    event_time,
    (event_time at time zone 'UTC')::date as event_date,
    type,
    wiki,
    server_name,
    title,
    user_name,
    -- Surrogate key; null when the event has no user (hidden by revision deletion).
    md5(user_name) as editor_id,
    loaded_at
from {{ source('raw', 'edits') }}
