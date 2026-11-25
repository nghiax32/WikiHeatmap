-- One row per Wikipedia language edition.
select
    wiki,
    min(server_name) as server_name,
    split_part(min(server_name), '.', 1) as language_code
from {{ ref('stg_edits') }}
group by wiki
