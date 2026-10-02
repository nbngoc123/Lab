{{ config(materialized='view') }}
{# MỚI: history trong get_team_data (xG, xGA, PPDA, deep, xpts... theo từng trận) - trước đây bị bỏ. #}

with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/understat/teams/**/*.json.gz') }}
),
teams as (
    select r.filename, e.value as t from raw r, json_each(r.doc) e
),
hist as (
    select filename, t, {{ jarray('t', '$.history') }} as h from teams
)

select
    {{ jget('t', '$.id', 'int') }}                   as team_id,
    {{ jget('t', '$.title') }}                       as team_name,
    {{ path_part('league') }}                        as league,
    {{ path_part('season') }}::int                   as season,
    try_cast(json_extract_string(h, '$.date') as timestamp) as match_date,
    {{ jget('h', '$.h_a') }}                         as home_away,       -- 'h' | 'a'
    {{ jget('h', '$.result') }}                      as result,          -- w | d | l
    {{ jget('h', '$.scored', 'int') }}               as scored,
    {{ jget('h', '$.missed', 'int') }}               as missed,
    {{ jget('h', '$.xG', 'double') }}                as xg,
    {{ jget('h', '$.xGA', 'double') }}               as xga,
    {{ jget('h', '$.npxG', 'double') }}              as npxg,
    {{ jget('h', '$.npxGA', 'double') }}             as npxga,
    {{ jget('h', '$.npxGD', 'double') }}             as npxgd,
    {{ jget('h', '$.deep', 'int') }}                 as deep,
    {{ jget('h', '$.deep_allowed', 'int') }}         as deep_allowed,
    {{ jget('h', '$.ppda.att', 'double') }}          as ppda_att,
    {{ jget('h', '$.ppda.def', 'double') }}          as ppda_def,
    {{ jget('h', '$.ppda_allowed.att', 'double') }}  as ppda_allowed_att,
    {{ jget('h', '$.ppda_allowed.def', 'double') }}  as ppda_allowed_def,
    {{ jget('h', '$.xpts', 'double') }}              as xpts,
    {{ jget('h', '$.pts', 'int') }}                  as pts,
    {{ path_date() }}                                as ingest_date
from hist
where json_extract_string(t, '$.id') is not null
qualify row_number() over (
    partition by team_id, league, season, match_date
    order by ingest_date desc nulls last, filename desc) = 1
