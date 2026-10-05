
with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/football_data_org/competitions/**/*.json.gz') }}
),
items as (select filename, {{ jarray('doc', '$.competitions') }} as c from raw)

select
    {{ jget('c', '$.id', 'int') }}                          as competition_id,
    {{ jget('c', '$.code') }}                               as code,
    {{ jget('c', '$.name') }}                               as name,
    {{ jget('c', '$.area.name') }}                          as area,
    {{ jget('c', '$.type') }}                               as type,
    {{ jget('c', '$.currentSeason.id', 'int') }}            as current_season_id,
    {{ jget('c', '$.currentSeason.startDate', 'date') }}    as current_season_start,
    {{ jget('c', '$.currentSeason.endDate', 'date') }}      as current_season_end,
    {{ jget('c', '$.currentSeason.currentMatchday', 'int') }} as current_matchday,
    {{ path_date() }}                                       as ingest_date
from items
where json_extract_string(c, '$.id') is not null
qualify row_number() over (partition by competition_id order by ingest_date desc nulls last, filename desc) = 1
