{{ config(
    materialized='view'
) }}

with raw_data as (
    select
        raw_data,
        ingest_timestamp
    from {{ source('raw', 'api_football_teams') }}
),

flattened as (
    select
        ingest_timestamp,
        value as team_obj
    from raw_data,
    lateral flatten(input => raw_data:response)
)

select
    team_obj:team:id::int as team_id,
    team_obj:team:name::varchar as name,
    team_obj:team:code::varchar as code,
    team_obj:team:country::varchar as country,
    team_obj:team:founded::int as founded,
    team_obj:team:national::boolean as is_national,
    team_obj:team:logo::varchar as logo_url,
    
    team_obj:venue:id::int as venue_id,
    team_obj:venue:name::varchar as venue_name,
    team_obj:venue:city::varchar as venue_city,
    team_obj:venue:capacity::int as venue_capacity,
    
    ingest_timestamp
from flattened
where team_id is not null
