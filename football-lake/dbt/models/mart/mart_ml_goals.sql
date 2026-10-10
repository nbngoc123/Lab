{{ config(materialized='external') }}
{# ML mart: 1 dòng / BÀN THẮNG (OpenLigaDB, chỉ các giải có trong p26). Bài toán: thời điểm ghi bàn, ai ghi bàn, "bàn kế tiếp",
   hiệu ứng game-state. Tỉ số TRƯỚC bàn suy từ tỉ số sau bàn của chính sự kiện.
   f_* = biết trước khi bàn xảy ra (phút, tỉ số trước, bối cảnh); y_* = đặc điểm của bàn. #}

with g as (
    select g.*, m.home_key, m.away_key
    from {{ ref('fact_goal') }} g
    join {{ ref('fact_match') }} m on m.match_key = g.match_key
),
b as (
    select *,
        case scoring_side when 'home' then home_score_after - 1 else home_score_after end as home_before,
        case scoring_side when 'away' then away_score_after - 1 else away_score_after end as away_before
    from g
    where scoring_side in ('home', 'away')
)
select
    goal_id, match_key, competition_key, season, match_date,
    home_key, away_key, scoring_team_key, scoring_side, scorer_id, scorer_name,

    -- ===== đặc trưng: trạng thái ngay trước bàn =====
    minute                                                              as f_minute,
    least(greatest(minute, 0) / 15, 5)                                  as f_minute_bucket,
    home_before                                                         as f_home_goals_before,
    away_before                                                         as f_away_goals_before,
    case scoring_side when 'home' then home_before - away_before
         else away_before - home_before end                             as f_scorer_diff_before,
    (scoring_side = 'home')::int                                        as f_scorer_is_home,

    -- ===== nhãn: tính chất của bàn =====
    case when (case scoring_side when 'home' then home_before - away_before else away_before - home_before end) < 0
         then 'trailing' when (case scoring_side when 'home' then home_before - away_before else away_before - home_before end) = 0
         then 'tied' else 'leading' end                                 as y_state_before_goal,
    ((case scoring_side when 'home' then home_before - away_before else away_before - home_before end) = -1)::int as y_is_equalizer,
    ((case scoring_side when 'home' then home_before - away_before else away_before - home_before end) = 0)::int  as y_is_go_ahead,
    is_penalty::int                                                     as y_is_penalty,
    is_own_goal::int                                                    as y_is_own_goal,
    (minute >= 75)::int                                                 as y_is_late_goal
from b
