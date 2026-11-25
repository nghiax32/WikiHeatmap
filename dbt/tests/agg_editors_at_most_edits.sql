-- A page cannot have more distinct editors than edits in a period.
{% for g in ['day', 'week', 'month'] %}
select '{{ g }}' as grain, period_start, wiki, title, editors, edits
from {{ ref('agg_page_' ~ g) }}
where editors > edits
{% if not loop.last %}union all{% endif %}
{% endfor %}
