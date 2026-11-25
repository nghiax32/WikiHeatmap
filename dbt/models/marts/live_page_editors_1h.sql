-- The last hour: the sliding window that ends right after the newest edit.
-- An edit at t belongs to every window ending in (t, t + 1 hour], so the window
-- with the largest end holds only the newest minutes; the one wanted ends at
-- (largest end) - 1 hour + slide. A view, so it is as fresh as the streaming sink.
{{ config(materialized='view') }}

select p.window_start, p.window_end, p.wiki, p.title, p.editors, p.edits, p.updated_at
from {{ source('realtime', 'page_editors_1h') }} p
where p.window_end = (
    select max(window_end) - interval '1 hour' + interval '{{ var("slide_minutes") }} minutes'
    from {{ source('realtime', 'page_editors_1h') }}
)
