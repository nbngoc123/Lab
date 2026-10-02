{# MỚI: ingest có ghi standings nhưng trước đây chưa có staging. standings là mảng-của-mảng (nhóm -> đội). #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/api_football/standings/**/*.json.gz') }}, filename=true)
),

leagues as (
    select filename, {{ jarray('doc', '$.response') }} as l from raw
),

grp as (
    select filename, l, unnest(from_json(json_extract(l, '$.league.standings'), '["JSON"]')) as g
    from leagues
),

teams as (
    select filename, l, unnest(from_json(g, '["JSON"]')) as t from grp
)

select
    {{ jget('l', '$.league.id', 'int') }}        as league_id,
    {{ jget('l', '$.league.season', 'int') }}    as season,
    {{ jget('t', '$.group') }}                   as group_name,
    {{ jget('t', '$.rank', 'int') }}             as rank,
    {{ jget('t', '$.team.id', 'int') }}          as team_id,
    {{ jget('t', '$.team.name') }}               as team,
    {{ jget('t', '$.points', 'int') }}           as points,
    {{ jget('t', '$.goalsDiff', 'int') }}        as goal_diff,
    {{ jget('t', '$.form') }}                    as form,
    {{ jget('t', '$.status') }}                  as trend,
    {{ jget('t', '$.description') }}             as description,
    {{ jget('t', '$.all.played', 'int') }}       as played,
    {{ jget('t', '$.all.win', 'int') }}          as win,
    {{ jget('t', '$.all.draw', 'int') }}         as draw,
    {{ jget('t', '$.all.lose', 'int') }}         as lose,
    {{ jget('t', '$.all.goals.for', 'int') }}    as goals_for,
    {{ jget('t', '$.all.goals.against', 'int') }} as goals_against,
    {{ path_date() }}                            as ingest_date
from teams
where json_extract_string(t, '$.team.id') is not null
qualify row_number() over (
    partition by league_id, season, coalesce(group_name, ''), team_id
    order by ingest_date desc nulls last, filename desc) = 1
