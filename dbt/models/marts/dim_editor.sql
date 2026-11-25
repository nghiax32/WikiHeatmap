-- One row per editor, only to count distinct editors and tell account types
-- apart. No per-editor metrics: the project ranks pages, not people.
select distinct
    editor_id,
    user_name,
    -- Temporary accounts of logged-out editors are named ~YYYY-...
    case when user_name like '~%' then 'temporary' else 'named' end as account_type
from {{ ref('stg_edits') }}
where editor_id is not null
