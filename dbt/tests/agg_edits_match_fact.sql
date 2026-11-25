-- Every edit is counted once in each grain: per-grain totals equal fact_edits.
with fact as (select count(*) as n from {{ ref('fact_edits') }})
{% for g in ['day', 'week', 'month'] %}
select '{{ g }}' as grain, (select n from fact) as fact_edits, sum(edits) as agg_edits
from {{ ref('agg_page_' ~ g) }}
having sum(edits) <> (select n from fact)
{% if not loop.last %}union all{% endif %}
{% endfor %}
