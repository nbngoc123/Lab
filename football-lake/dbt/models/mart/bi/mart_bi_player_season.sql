{{ config(materialized='external') }}
{# BI mart: Thống kê hiệu suất cầu thủ theo mùa giải (Grain: player_key + competition_key + season + team_key).
   Kết hợp số liệu fact_player_season từ Understat với thông tin thực thể dim_player.
   Phục vụ các câu hỏi: Hiệu suất cầu thủ, so sánh theo vị trí thi đấu, xu hướng theo mùa giải. #}

with stats as (
    select * from {{ ref('fact_player_season') }}
),

players as (
    select
        player_key,
        player_name,
        nationality,
        date_of_birth,
        photo_url
    from {{ ref('dim_player') }}
)

select
    s.player_key,
    coalesce(p.player_name, s.player_key) as player_name,
    p.nationality,
    p.date_of_birth,
    s.competition_key,
    s.season,
    s.team_key,
    s.position,
    s.games,
    s.minutes_played,
    s.goals,
    s.assists,
    s.goals + s.assists as goal_contributions,
    s.shots,
    s.key_passes,
    s.yellow_cards,
    s.red_cards,
    s.non_penalty_goals,
    s.xg,
    s.xa,
    s.xg + s.xa as xg_contributions,
    s.non_penalty_xg,
    s.xg_chain,
    s.xg_buildup,
    s.goals_per90,
    s.xg_per90,
    s.xa_per90,
    case when s.shots > 0 then round(s.goals * 1.0 / s.shots, 3) end as shot_conversion_rate,
    p.photo_url
from stats s
left join players p on s.player_key = p.player_key
