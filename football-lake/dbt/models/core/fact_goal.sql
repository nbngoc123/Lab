{{ config(materialized='external') }}
{# 1 dòng / bàn thắng (nguồn OpenLigaDB): phút, cầu thủ, đội được tính bàn, penalty/phản lưới.
   Gắn vào trận ở fact_match qua openliga_match_id nên join được với mọi nguồn khác bằng match_key.
   scoring_side: 'home'/'away' = đội được TÍNH bàn (bàn phản lưới: đội hưởng lợi). #}

select
    g.goal_id,
    fm.match_key,
    fm.competition_key,
    fm.season,
    fm.match_date,
    g.minute,
    g.scorer_id,
    g.scorer_name,
    {{ team_key('t.team_name') }}                                    as scoring_team_key,
    case when g.scoring_team_id = m.home_team_id then 'home'
         when g.scoring_team_id = m.away_team_id then 'away' end     as scoring_side,
    g.is_penalty,
    g.is_own_goal,
    g.is_overtime,
    g.score_team1                                                    as home_score_after,
    g.score_team2                                                    as away_score_after
from {{ ref('stg_openliga_goals') }} g
join {{ ref('int_openliga_matches') }} m on m.match_id = g.match_id
join {{ ref('fact_match') }} fm on fm.openliga_match_id = cast(g.match_id as varchar)
left join {{ ref('stg_openliga_teams') }} t on t.team_id = g.scoring_team_id
