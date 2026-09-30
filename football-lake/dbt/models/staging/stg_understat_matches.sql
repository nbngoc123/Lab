{{ config(
    materialized='view'
) }}

with raw_data as (
    select *
    from read_json_auto('s3://football-lake/raw/understat/matches/**/*.json.gz')
)

select
    id::int as match_id,
    isResult::boolean as is_result,
    
    h.id::int as home_team_id,
    h.title::varchar as home_team_name,
    a.id::int as away_team_id,
    a.title::varchar as away_team_name,
    
    goals.h::int as home_goals,
    goals.a::int as away_goals,
    
    xG.h::float as home_xg,
    xG.a::float as away_xg,
    
    datetime::timestamp as match_date
    
from raw_data
