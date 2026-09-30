{{ config(materialized='view') }}
{# Glob trúng cả 2 layout: teams/league=X/season=Y/ và teams/season=Y/ (module cũ). Dedup theo (team, season). #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/api_football/teams/**/*.json.gz') }}, filename=true)
),

items as (
    select filename, {{ jarray('doc', '$.response') }} as t from raw
)

select
    {{ jget('t', '$.team.id', 'int') }}            as team_id,
    {{ jget('t', '$.team.name') }}                 as name,
    {{ jget('t', '$.team.code') }}                 as code,
    {{ jget('t', '$.team.country') }}              as country,
    {{ jget('t', '$.team.founded', 'int') }}       as founded,
    {{ jget('t', '$.team.national', 'boolean') }}  as is_national,
    {{ jget('t', '$.team.logo') }}                 as logo_url,
    {{ jget('t', '$.venue.id', 'int') }}           as venue_id,
    {{ jget('t', '$.venue.name') }}                as venue_name,
    {{ jget('t', '$.venue.city') }}                as venue_city,
    {{ jget('t', '$.venue.capacity', 'int') }}     as venue_capacity,
    {{ path_part('league') }}::int                 as league_id,
    {{ path_part('season') }}::int                 as season
from items
where json_extract_string(t, '$.team.id') is not null
qualify row_number() over (
    partition by team_id, season
    order by (league_id is null), filename desc) = 1
