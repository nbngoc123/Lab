{{ config(materialized='view') }}

select
    match_id, sport_key, home_team, away_team, commence_time,
    bookmaker_key, bookmaker, last_update, snapshot_date,
    outcome_name  as team_name,
    point         as handicap_point,
    price
from {{ ref('stg_odds_base') }}
where market_key = 'spreads'
qualify row_number() over (
    partition by match_id, bookmaker_key, outcome_name
    order by last_update desc nulls last, snapshot_date desc nulls last) = 1
