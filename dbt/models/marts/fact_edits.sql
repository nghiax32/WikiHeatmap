-- One row per human article edit. Incremental on loaded_at: a run picks up
-- whatever load_postgres.py (re)loaded since the previous run, including a
-- reloaded old day, and delete+insert on event_id replaces those rows.
{{ config(materialized='incremental', unique_key='event_id',
          incremental_strategy='delete+insert') }}

with edits as (
    select * from {{ ref('stg_edits') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

-- The producer resends events after a restart (same event_id).
deduped as (
    select *, row_number() over (partition by event_id order by loaded_at) as copy
    from edits
)

select event_id, event_time, event_date, type, wiki, title, editor_id, loaded_at
from deduped
where copy = 1
