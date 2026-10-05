{# Trạng thái hiện tại của bảng leagues (CDC từ OpenLigaDB). 1 dòng / league_id (mỗi league = 1 giải x 1 mùa). #}

select
    {{ jget('a', '$.league_id', 'int') }}   as league_id,
    {{ jget('a', '$.shortcut') }}           as league_shortcut,
    {{ jget('a', '$.season', 'int') }}      as season,
    {{ jget('a', '$.name') }}               as league_name,
    {{ jget('a', '$.sport_id', 'int') }}    as sport_id
from {{ cdc_current('leagues', 'league_id') }} as c
where {{ jget('a', '$.season', 'int') }} >= {{ var('openliga_min_season', 2024) }}
