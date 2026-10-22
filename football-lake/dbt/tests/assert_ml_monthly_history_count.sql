{{ config(tags=['ml_monthly']) }}

-- A regression guard against counting the current or future matches as history.
-- It complements, rather than proves, the point-in-time checks on each source.
with observed as (
    select t.*,
        coalesce(cast(t.kickoff_utc as timestamp), cast(t.match_date as timestamp)) as prediction_ts
    from {{ ref('mart_ml_team_match_features') }} t
    join {{ ref('mart_ml_training_snapshot') }} s on s.match_key = t.match_key
),
history as (
    select f.team_key,
        case when f.kickoff_utc is not null then cast(f.kickoff_utc as timestamp) + interval 3 hour
             else cast(f.match_date as timestamp) + interval 23 hour + interval 59 minute + interval 59 second
        end as known_ts
    from {{ ref('fact_team_match') }} f
    where f.team_key is not null and f.is_finished
      and f.goals_for is not null and f.goals_against is not null
      and f.match_date >= date '{{ var("ml_min_match_date", "2000-01-01") }}'
),
allowed as (
    select o.team_match_key, count(h.team_key) as allowed_count
    from observed o left join history h
      on h.team_key = o.team_key and h.known_ts < o.prediction_ts
    group by o.team_match_key
)
select o.team_match_key, o.f_n_prev, a.allowed_count
from observed o join allowed a on a.team_match_key = o.team_match_key
where o.f_n_prev is null or o.f_n_prev < 0 or o.f_n_prev > a.allowed_count
