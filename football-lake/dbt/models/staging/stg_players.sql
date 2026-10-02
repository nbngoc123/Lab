{{ config(materialized='view') }}

with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/api_football/players_summary/**/*.json.gz') }}
),

items as (
    select filename, {{ jarray('doc', '$.response') }} as p from raw
)

select
    {{ jget('p', '$.player.id', 'int') }}             as player_id,
    {{ jget('p', '$.player.name') }}                  as name,
    {{ jget('p', '$.player.firstname') }}             as firstname,
    {{ jget('p', '$.player.lastname') }}              as lastname,
    {{ jget('p', '$.player.age', 'int') }}            as age,
    {{ jget('p', '$.player.birth.date', 'date') }}    as birth_date,
    {{ jget('p', '$.player.birth.country') }}         as birth_country,
    {{ jget('p', '$.player.nationality') }}           as nationality,
    {{ jget('p', '$.player.height') }}                as height,
    {{ jget('p', '$.player.weight') }}                as weight,
    {{ jget('p', '$.player.injured', 'boolean') }}    as is_injured,
    {{ jget('p', '$.player.photo') }}                 as photo_url,
    {{ jget('p', '$.statistics[0].team.id', 'int') }} as team_id,
    {{ jget('p', '$.statistics[0].team.name') }}      as team_name,
    {{ jget('p', '$.statistics[0].games.position') }} as position,
    {{ jget('p', '$.statistics[0].league.id', 'int') }} as league_id,
    {{ jget('p', '$.statistics[0].league.season', 'int') }} as season,
    {{ path_date() }}                                 as ingest_date
from items
where json_extract_string(p, '$.player.id') is not null
qualify row_number() over (
    partition by player_id, season
    order by ingest_date desc nulls last) = 1
