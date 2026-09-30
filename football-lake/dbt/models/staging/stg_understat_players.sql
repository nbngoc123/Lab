{{ config(materialized='view') }}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/understat/players/**/*.json.gz') }}, filename=true)
)

select
    {{ jget('doc', '$.id', 'int') }}                 as player_id,
    {{ path_part('league') }}                        as league,
    {{ path_part('season') }}::int                   as season,
    {{ jget('doc', '$.player_name') }}               as player_name,
    {{ jget('doc', '$.games', 'int') }}              as games,
    {{ jget('doc', '$.time', 'int') }}               as minutes_played,
    {{ jget('doc', '$.goals', 'int') }}              as goals,
    {{ jget('doc', '$.xG', 'double') }}              as xg,
    {{ jget('doc', '$.assists', 'int') }}            as assists,
    {{ jget('doc', '$.xA', 'double') }}              as xa,
    {{ jget('doc', '$.shots', 'int') }}              as shots,
    {{ jget('doc', '$.key_passes', 'int') }}         as key_passes,
    {{ jget('doc', '$.yellow_cards', 'int') }}       as yellow_cards,
    {{ jget('doc', '$.red_cards', 'int') }}          as red_cards,
    {{ jget('doc', '$.position') }}                  as position,
    {{ jget('doc', '$.team_title') }}                as team_name,      -- có thể là "A,B" nếu chuyển đội giữa mùa
    {{ jget('doc', '$.npg', 'int') }}                as non_penalty_goals,
    {{ jget('doc', '$.npxG', 'double') }}            as non_penalty_xg,
    {{ jget('doc', '$.xGChain', 'double') }}         as xg_chain,
    {{ jget('doc', '$.xGBuildup', 'double') }}       as xg_buildup,
    {{ path_date() }}                                as ingest_date
from raw
where json_extract_string(doc, '$.id') is not null
qualify row_number() over (
    partition by player_id, league, season
    order by ingest_date desc nulls last, filename desc) = 1
