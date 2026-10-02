{# Thêm league/season từ path (trước đây mất => không phân biệt được mùa). Dedup theo snapshot mới nhất. #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/understat/matches/**/*.json.gz') }}, filename=true)
)

select
    {{ jget('doc', '$.id', 'int') }}            as match_id,
    {{ path_part('league') }}                   as league,
    {{ path_part('season') }}::int              as season,
    {{ jget('doc', '$.isResult', 'boolean') }}  as is_result,
    {{ jget('doc', '$.h.id', 'int') }}          as home_team_id,
    {{ jget('doc', '$.h.title') }}              as home_team_name,
    {{ jget('doc', '$.a.id', 'int') }}          as away_team_id,
    {{ jget('doc', '$.a.title') }}              as away_team_name,
    {{ jget('doc', '$.goals.h', 'int') }}       as home_goals,
    {{ jget('doc', '$.goals.a', 'int') }}       as away_goals,
    {{ jget('doc', '$.xG.h', 'double') }}       as home_xg,
    {{ jget('doc', '$.xG.a', 'double') }}       as away_xg,
    {{ jget('doc', '$.forecast.w', 'double') }} as forecast_home_win,
    {{ jget('doc', '$.forecast.d', 'double') }} as forecast_draw,
    {{ jget('doc', '$.forecast.l', 'double') }} as forecast_away_win,
    try_cast(json_extract_string(doc, '$.datetime') as timestamp) as match_date,
    {{ path_date() }}                           as ingest_date
from raw
where json_extract_string(doc, '$.id') is not null
qualify row_number() over (
    partition by match_id, league, season
    order by ingest_date desc nulls last, filename desc) = 1
