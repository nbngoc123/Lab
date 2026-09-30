{{ config(
    materialized='view'
) }}

with raw_data as (
    select *
    from read_json_auto('s3://football-lake/raw/api_football/players_summary/**/*.json.gz')
),

flattened as (
    select
        unnest(from_json(response, '["JSON"]')) as player_obj
    from raw_data
)

select
    (player_obj->>'$.player.id')::int as player_id,
    (player_obj->>'$.player.name')::varchar as name,
    (player_obj->>'$.player.firstname')::varchar as firstname,
    (player_obj->>'$.player.lastname')::varchar as lastname,
    (player_obj->>'$.player.age')::int as age,
    (player_obj->>'$.player.nationality')::varchar as nationality,
    (player_obj->>'$.player.height')::varchar as height,
    (player_obj->>'$.player.weight')::varchar as weight,
    (player_obj->>'$.player.injured')::boolean as is_injured,
    (player_obj->>'$.player.photo')::varchar as photo_url
    
from flattened
where (player_obj->>'$.player.id') is not null
