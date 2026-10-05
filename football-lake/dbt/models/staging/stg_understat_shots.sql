
{# Body {"h":[...],"a":[...]}. Path: shots/league=/season=/match_id=/shots.json.gz (không có ingest_date, 1 file/trận). #}

with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/understat/shots/**/*.json.gz') }}
),

sides as (
    select filename, 'h' as side, {{ jarray('doc', '$.h') }} as s from raw
    union all
    select filename, 'a' as side, {{ jarray('doc', '$.a') }} as s from raw
)

select
    {{ jget('s', '$.id') }}                   as shot_id,
    {{ jget('s', '$.match_id', 'int') }}      as match_id,
    {{ path_part('league') }}                 as league,
    {{ path_part('season') }}::int            as season,
    side,
    case side when 'h' then {{ jget('s', '$.h_team') }} else {{ jget('s', '$.a_team') }} end as team_name,
    {{ jget('s', '$.minute', 'int') }}        as minute,
    {{ jget('s', '$.X', 'double') }}          as x_coord,
    {{ jget('s', '$.Y', 'double') }}          as y_coord,
    {{ jget('s', '$.xG', 'double') }}         as xg,
    {{ jget('s', '$.result') }}               as result,
    {{ jget('s', '$.player') }}               as player_name,
    {{ jget('s', '$.player_id', 'int') }}     as player_id,
    {{ jget('s', '$.player_assisted') }}      as assisted_by,
    {{ jget('s', '$.lastAction') }}           as last_action,
    {{ jget('s', '$.situation') }}            as situation,
    {{ jget('s', '$.shotType') }}             as shot_type
from sides
where json_extract_string(s, '$.id') is not null
qualify row_number() over (partition by shot_id order by filename desc) = 1
