{{ config(materialized='view', enabled=var('has_fdo_teams', false)) }}
{# Squad nằm trong body teams; bật cùng has_fdo_teams. Giữ competition_code để phân biệt CLB ở PL vs CL. #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/football_data_org/teams/**/*.json.gz') }}, filename=true)
),
teams as (
    select filename, {{ jget('doc', '$.competition.code') }} as body_comp, {{ jarray('doc', '$.teams') }} as t from raw
),
squad as (
    select
        filename, body_comp,
        {{ jget('t', '$.id', 'int') }} as team_id,
        {{ jget('t', '$.name') }}      as team_name,
        {{ jarray('t', '$.squad') }}   as p
    from teams
)

select
    {{ jget('p', '$.id', 'int') }}              as player_id,
    {{ jget('p', '$.name') }}                   as name,
    {{ jget('p', '$.position') }}               as position,
    {{ jget('p', '$.dateOfBirth', 'date') }}    as date_of_birth,
    {{ jget('p', '$.nationality') }}            as nationality,
    team_id, team_name,
    coalesce({{ path_part('comp') }}, body_comp) as competition_code,
    {{ path_date() }}                           as ingest_date
from squad
where json_extract_string(p, '$.id') is not null
qualify row_number() over (partition by player_id, team_id order by ingest_date desc nulls last, filename desc) = 1
