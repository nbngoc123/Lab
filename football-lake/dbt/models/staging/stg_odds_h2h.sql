
select
    match_id, sport_key, home_team, away_team, commence_time,
    bookmaker_key, bookmaker, last_update, snapshot_date,
    outcome_name,
    case
        when outcome_name = home_team then 'home'
        when outcome_name = away_team then 'away'
        when lower(outcome_name) = 'draw' then 'draw'
    end as outcome_type,
    price
from {{ ref('stg_odds_base') }}
where market_key = 'h2h'
qualify row_number() over (
    partition by match_id, bookmaker_key, outcome_name
    order by last_update desc nulls last, snapshot_date desc nulls last) = 1
