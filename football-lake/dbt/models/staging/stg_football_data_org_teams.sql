
with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/football_data_org/teams/**/*.json.gz') }}
),
items as (
    select filename, {{ jget('doc', '$.competition.code') }} as body_comp, {{ jarray('doc', '$.teams') }} as t from raw
)

select
    {{ jget('t', '$.id', 'int') }}                    as team_id,
    coalesce({{ path_part('comp') }}, body_comp)      as competition_code,
    {{ jget('t', '$.name') }}                         as name,
    {{ jget('t', '$.shortName') }}                    as short_name,
    {{ jget('t', '$.tla') }}                          as tla,
    {{ jget('t', '$.founded', 'int') }}               as founded,
    {{ jget('t', '$.venue') }}                        as venue,
    {{ jget('t', '$.clubColors') }}                   as club_colors,
    {{ jget('t', '$.area.name') }}                    as area,
    {{ jget('t', '$.coach.name') }}                   as coach_name,
    {{ jget('t', '$.coach.nationality') }}            as coach_nationality,
    {{ path_date() }}                                 as ingest_date
from items
where json_extract_string(t, '$.id') is not null
qualify row_number() over (partition by team_id, competition_code order by ingest_date desc nulls last, filename desc) = 1
