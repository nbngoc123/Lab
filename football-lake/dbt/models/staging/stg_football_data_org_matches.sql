{{ config(materialized='view') }}
{# Body lỗi (403/429 bị p09 lưu như data) không có key 'matches' => 0 dòng thay vì vỡ. #}

with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/football_data_org/matches/**/*.json.gz') }}
),

items as (
    select filename, {{ jarray('doc', '$.matches') }} as m from raw
)

select
    {{ jget('m', '$.id', 'int') }}                       as match_id,
    {{ path_part('comp') }}                              as competition_code,
    {{ jget('m', '$.season.id', 'int') }}                as season_id,
    {{ jget('m', '$.matchday', 'int') }}                 as matchday,
    {{ jget('m', '$.stage') }}                           as stage,
    {{ jget('m', '$.group') }}                           as group_name,
    {{ jts('m', '$.utcDate') }}                          as utc_date,
    {{ jget('m', '$.status') }}                          as status,
    {{ jts('m', '$.lastUpdated') }}                      as last_updated,
    {{ jget('m', '$.homeTeam.id', 'int') }}              as home_team_id,
    {{ jget('m', '$.homeTeam.name') }}                   as home_team,
    {{ jget('m', '$.homeTeam.tla') }}                    as home_tla,
    {{ jget('m', '$.awayTeam.id', 'int') }}              as away_team_id,
    {{ jget('m', '$.awayTeam.name') }}                   as away_team,
    {{ jget('m', '$.awayTeam.tla') }}                    as away_tla,
    {{ jget('m', '$.score.fullTime.home', 'int') }}      as home_goals,
    {{ jget('m', '$.score.fullTime.away', 'int') }}      as away_goals,
    {{ jget('m', '$.score.halfTime.home', 'int') }}      as ht_home_goals,
    {{ jget('m', '$.score.halfTime.away', 'int') }}      as ht_away_goals,
    {{ jget('m', '$.score.winner') }}                    as winner,
    {{ jget('m', '$.score.duration') }}                  as duration,
    {{ jget('m', '$.referees[0].name') }}                as referee,
    {{ path_date() }}                                    as ingest_date
from items
where json_extract_string(m, '$.id') is not null
qualify row_number() over (
    partition by match_id, competition_code
    order by ingest_date desc nulls last, filename desc) = 1
