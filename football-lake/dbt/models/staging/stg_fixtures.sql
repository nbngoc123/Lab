{{ config(
    materialized='view'
) }}

with raw_data as (
    select *
    from read_json_auto('s3://football-lake/raw/api_football/fixtures/**/*.json.gz')
),

flattened as (
    select
        unnest(from_json(response, '["JSON"]')) as fixture_obj
    from raw_data
)

select
    -- Fixture info
    (fixture_obj->>'$.fixture.id')::int as fixture_id,
    (fixture_obj->>'$.fixture.date')::timestamp as fixture_date,
    (fixture_obj->>'$.fixture.status.long')::varchar as status_long,
    (fixture_obj->>'$.fixture.status.short')::varchar as status_short,
    
    -- League info
    (fixture_obj->>'$.league.id')::int as league_id,
    (fixture_obj->>'$.league.season')::int as season,
    (fixture_obj->>'$.league.round')::varchar as round,
    
    -- Team info
    (fixture_obj->>'$.teams.home.id')::int as home_team_id,
    (fixture_obj->>'$.teams.home.name')::varchar as home_team_name,
    (fixture_obj->>'$.teams.away.id')::int as away_team_id,
    (fixture_obj->>'$.teams.away.name')::varchar as away_team_name,
    
    -- Goals and Score
    (fixture_obj->>'$.goals.home')::int as home_goals,
    (fixture_obj->>'$.goals.away')::int as away_goals
    
from flattened
where (fixture_obj->>'$.fixture.id') is not null
