{{ config(materialized='view', enabled=var('has_thesportsdb_players', false)) }}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/thesportsdb/metadata/entity=honors/**/*.json.gz') }}, filename=true)
),
items as (select filename, {{ jarray('doc', '$.honors') }} as h from raw)

select
    {{ jget('h', '$.idPlayer') }}   as player_id,
    {{ jget('h', '$.strHonour') }}  as honor,
    {{ jget('h', '$.strSeason') }}  as season,
    {{ path_date() }}               as ingest_date
from items
where json_extract_string(h, '$.idPlayer') is not null
qualify row_number() over (
    partition by player_id, honor, coalesce(season, '')
    order by ingest_date desc nulls last, filename desc) = 1
