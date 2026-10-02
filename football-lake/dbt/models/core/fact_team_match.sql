{{ config(materialized='table') }}
{# 1 trận -> 2 dòng (đội nhà, đội khách). Bảng dài, tiện tính rolling form / xG cho ML.
   Bổ sung PPDA, deep, xpts của Understat theo (đội, ngày, sân). #}

with m as (select * from {{ ref('fact_match') }}),

sides as (
    select match_key, competition_key, season, match_date, kickoff_utc, 'h' as side,
           home_key as team_key, away_key as opponent_key,
           home_goals as goals_for, away_goals as goals_against,
           home_xg as xg_for, away_xg as xg_against,
           home_shots as shots, home_shots_target as shots_target, home_corners as corners,
           home_fouls as fouls, home_yellows as yellows, home_reds as reds, is_finished
    from m
    union all
    select match_key, competition_key, season, match_date, kickoff_utc, 'a',
           away_key, home_key, away_goals, home_goals, away_xg, home_xg,
           away_shots, away_shots_target, away_corners, away_fouls, away_yellows, away_reds, is_finished
    from m
),

hist as (
    select
        {{ team_key('team_name') }}   as team_key,
        cast(match_date as date)      as match_date,
        home_away,
        ppda_att, ppda_def, deep, deep_allowed, xpts, npxg, npxga
    from {{ ref('stg_understat_team_matches') }}
    qualify row_number() over (partition by {{ team_key('team_name') }}, cast(match_date as date), home_away
                               order by match_date) = 1
)

select
    s.match_key || '|' || s.side                                  as team_match_key,
    s.match_key, s.competition_key, s.season, s.match_date, s.kickoff_utc,
    s.side, s.team_key, s.opponent_key, s.is_finished,
    s.goals_for, s.goals_against,
    case when s.is_finished and s.goals_for is not null then
        case when s.goals_for > s.goals_against then 3 when s.goals_for = s.goals_against then 1 else 0 end
    end                                                           as points,
    s.xg_for, s.xg_against,
    s.shots, s.shots_target, s.corners, s.fouls, s.yellows, s.reds,
    h.ppda_att, h.ppda_def, h.deep, h.deep_allowed, h.xpts, h.npxg, h.npxga
from sides s
left join hist h on h.team_key = s.team_key and h.match_date = s.match_date and h.home_away = s.side
