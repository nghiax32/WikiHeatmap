{#
  Distinct editors and edits per page per calendar period (UTC; date_trunc
  weeks start on Monday). Recomputed from fact_edits for every grain: distinct
  counts cannot be summed from a shorter period.
#}
{% macro page_editors(grain) %}
with counts as (
    select
        date_trunc('{{ grain }}', event_date)::date as period_start,
        wiki,
        title,
        count(distinct editor_id) as editors,
        count(*) as edits
    from {{ ref('fact_edits') }}
    group by 1, 2, 3
)

select
    period_start,
    (period_start + interval '1 {{ grain }}')::date as period_end,
    wiki,
    title,
    editors,
    edits,
    -- Same order as analysis/top_pages.py; row_number so ties do not exceed a top-K.
    row_number() over (partition by period_start, wiki
                       order by editors desc, edits desc, title) as rank_in_wiki
from counts
{% endmacro %}
