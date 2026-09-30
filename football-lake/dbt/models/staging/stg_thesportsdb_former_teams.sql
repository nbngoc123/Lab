{{ config(materialized='view', enabled=var('has_thesportsdb_players', false)) }}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/thesportsdb/metadata/entity=former_teams/**/*.json.gz') }}, filename=true)
),
items as (select filename, {{ jarray('doc', '$.formerteams') }} as f from raw)

select
    {{ jget('f', '$.idPlayer') }}      as player_id,
    {{ jget('f', '$.strFormerTeam') }} as former_team,
    {{ jget('f', '$.strJoined') }}     as start_year,
    {{ jget('f', '$.strDeparted') }}   as end_year,
    {{ path_date() }}                  as ingest_date
from items
where json_extract_string(f, '$.idPlayer') is not null
qualify row_number() over (
    partition by player_id, former_team, coalesce(start_year, '')
    order by ingest_date desc nulls last, filename desc) = 1
