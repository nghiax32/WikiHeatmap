-- All four windows in one shape, for the dashboard: pick window_type and a period.
{{ config(materialized='view') }}

with live as (
    select
        '1 hour'::text as window_type,
        window_start as period_start,
        window_end as period_end,
        wiki, title, editors, edits,
        row_number() over (partition by wiki order by editors desc, edits desc, title) as rank_in_wiki,
        false as is_complete
    from {{ ref('live_page_editors_1h') }}
),

-- A calendar period is complete when the loaded edits cover all of it.
coverage as (
    select min(event_time) as first_edit,
           date_trunc('day', max(event_time) + interval '1 second', 'UTC') as loaded_until
    from {{ ref('fact_edits') }}
)

select * from live
{% for g in ['day', 'week', 'month'] %}
union all
select
    '{{ g }}',
    a.period_start::timestamp at time zone 'UTC',
    a.period_end::timestamp at time zone 'UTC',
    a.wiki, a.title, a.editors, a.edits, a.rank_in_wiki,
    a.period_start::timestamp at time zone 'UTC' >= c.first_edit
        and a.period_end::timestamp at time zone 'UTC' <= c.loaded_until
from {{ ref('agg_page_' ~ g) }} a
cross join coverage c
{% endfor %}
