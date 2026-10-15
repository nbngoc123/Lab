{{ config(materialized='external') }}
{# BI mart: one row per (match_key, team_key), including unfinished fixtures.
   Reuse the team/opponent perspective and points from Core; no joins or deduplication.
   A result requires a finished match AND both scores. Missing statistics remain NULL.
   Only finished-match statistics are exposed; this is not a pre-match ML feature table.
   Count DISTINCT match_key for matches: each valid match has two team rows.
#}

with base as (
    select
        t.*,
        (t.is_finished and t.goals_for is not null and t.goals_against is not null) as has_result
    from {{ ref('fact_team_match') }} t
)

select
    -- Keys and context already supplied by Core.
    b.team_match_key,
    b.match_key,
    b.team_key,
    b.opponent_key,
    b.competition_key,
    b.season,
    b.match_date,
    b.kickoff_utc,
    b.side,
    (b.side = 'h') as is_home,
    b.is_finished,
    b.has_result,

    -- Final score and outcome from this team's perspective.
    -- Guard Core points as well: a partial score must not become a loss or zero points.
    case when b.has_result then b.goals_for end as goals_for,
    case when b.has_result then b.goals_against end as goals_against,
    case when b.has_result then b.goals_for - b.goals_against end as goal_difference,
    case when b.has_result then b.goals_for + b.goals_against end as total_goals,
    case when b.has_result then b.points end as points,
    case when b.has_result then
        case b.points when 3 then 'W' when 1 then 'D' when 0 then 'L' end
    end as team_result,
    case when b.has_result then (b.points = 3)::integer end as is_win,
    case when b.has_result then (b.points = 1)::integer end as is_draw,
    case when b.has_result then (b.points = 0)::integer end as is_loss,
    case when b.has_result then (b.goals_against = 0)::integer end as is_clean_sheet,

    -- Actual match statistics, not rolling features. Do not replace missing values with zero.
    case when b.is_finished then b.xg_for end as xg_for,
    case when b.is_finished then b.xg_against end as xg_against,
    case when b.is_finished then b.xg_for - b.xg_against end as xg_difference,
    case when b.has_result then b.goals_for - b.xg_for end as goals_minus_xg,
    case when b.is_finished then b.shots end as shots,
    case when b.is_finished then b.shots_target end as shots_target,
    case when b.is_finished then
        1.0 * b.shots_target / nullif(b.shots, 0)
    end as shot_accuracy,
    case when b.is_finished then b.corners end as corners,
    case when b.is_finished then b.fouls end as fouls,
    case when b.is_finished then b.yellows end as yellows,
    case when b.is_finished then b.reds end as reds,
    case when b.is_finished then b.ppda_att end as ppda_att,
    case when b.is_finished then b.ppda_def end as ppda_def,
    case when b.is_finished then
        b.ppda_att / nullif(b.ppda_def, 0)
    end as ppda,
    case when b.is_finished then b.deep end as deep,
    case when b.is_finished then b.deep_allowed end as deep_allowed,
    case when b.is_finished then b.xpts end as xpts,
    case when b.is_finished then b.npxg end as npxg,
    case when b.is_finished then b.npxga end as npxga
from base b
