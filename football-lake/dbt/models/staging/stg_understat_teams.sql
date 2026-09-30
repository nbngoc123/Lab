{{ config(materialized='view') }}
{# Body là dict động {"89": {id,title,history}, ...} => json_each() duyệt từng key. #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/understat/teams/**/*.json.gz') }}, filename=true)
),
teams as (
    select r.filename, e.value as t from raw r, json_each(r.doc) e
)

select
    {{ jget('t', '$.id', 'int') }}   as team_id,
    {{ jget('t', '$.title') }}       as team_name,
    {{ path_part('league') }}        as league,
    {{ path_part('season') }}::int   as season,
    {{ path_date() }}                as ingest_date
from teams
where json_extract_string(t, '$.id') is not null
qualify row_number() over (
    partition by team_id, league, season
    order by ingest_date desc nulls last, filename desc) = 1
