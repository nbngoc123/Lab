
with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/football_data_org/standings/**/*.json.gz') }}
),
st as (
    select
        filename,
        {{ jget('doc', '$.competition.code') }}   as body_comp,
        {{ jget('doc', '$.season.id', 'int') }}   as season_id,
        {{ jarray('doc', '$.standings') }}        as s
    from raw
),
tbl as (
    select filename, body_comp, season_id,
           {{ jget('s', '$.stage') }} as stage,
           {{ jarray('s', '$.table') }} as t
    from st
    where json_extract_string(s, '$.type') = 'TOTAL'
)

select
    coalesce({{ path_part('comp') }}, body_comp)    as competition_code,
    season_id, stage,
    {{ jget('t', '$.position', 'int') }}            as rank,
    {{ jget('t', '$.team.id', 'int') }}             as team_id,
    {{ jget('t', '$.team.name') }}                  as team,
    {{ jget('t', '$.playedGames', 'int') }}         as played,
    {{ jget('t', '$.won', 'int') }}                 as won,
    {{ jget('t', '$.draw', 'int') }}                as draw,
    {{ jget('t', '$.lost', 'int') }}                as lost,
    {{ jget('t', '$.points', 'int') }}              as points,
    {{ jget('t', '$.goalsFor', 'int') }}            as goals_for,
    {{ jget('t', '$.goalsAgainst', 'int') }}        as goals_against,
    {{ jget('t', '$.goalDifference', 'int') }}      as goal_diff,
    {{ jget('t', '$.form') }}                       as form,
    {{ path_date() }}                               as ingest_date
from tbl
where json_extract_string(t, '$.team.id') is not null
qualify row_number() over (
    partition by competition_code, season_id, coalesce(stage, ''), team_id
    order by ingest_date desc nulls last, filename desc) = 1
