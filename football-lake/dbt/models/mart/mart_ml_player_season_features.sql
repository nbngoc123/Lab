{{ config(materialized='external') }}
{# ML mart: 1 dòng / (cầu thủ, mùa, giải, đội) - Understat. Bài toán: dự đoán hiệu suất mùa này từ thông tin TRƯỚC mùa.
   - f_* : tuổi tại 1/8 đầu mùa, vị trí, chiều cao, giải, và thống kê của các mùa TRƯỚC (f_prev1_*, f_prev2_*, gộp mọi giải/đội).
   - y_* : kết quả của chính mùa này (SAU). season_complete=false (mùa đang diễn ra) -> y_* chưa đầy đủ, đừng train trên đó.
   - is_reliable_target: mùa đã xong và >= 900 phút (per90 đủ ổn định). #}

with p as (select * from {{ ref('fact_player_season') }}),
dp as (select player_key, date_of_birth, nationality, height_cm from {{ ref('dim_player') }}),
tot as (
    select player_key, season,
        sum(minutes_played) as minutes, sum(games) as games, sum(goals) as goals, sum(assists) as assists,
        sum(xg) as xg, sum(xa) as xa, sum(shots) as shots, sum(key_passes) as key_passes
    from p
    group by player_key, season
),
tot90 as (
    select *,
        case when minutes >= 450 then goals * 90.0 / minutes end      as goals_p90,
        case when minutes >= 450 then xg * 90.0 / minutes end         as xg_p90,
        case when minutes >= 450 then assists * 90.0 / minutes end    as assists_p90,
        case when minutes >= 450 then xa * 90.0 / minutes end         as xa_p90,
        case when minutes >= 450 then shots * 90.0 / minutes end      as shots_p90,
        case when minutes >= 450 then key_passes * 90.0 / minutes end as key_passes_p90
    from tot
)
select
    p.player_key, p.competition_key, p.season, p.team_key, p.position,

    -- ===== đặc trưng (trước mùa) =====
    case when dp.date_of_birth is not null
         then date_diff('day', cast(dp.date_of_birth as date), make_date(p.season, 8, 1)) / 365.25 end as f_age,
    dp.height_cm                                       as f_height_cm,
    dp.nationality                                     as nationality,
    dc.tier                                            as f_comp_tier,
    a.minutes as f_prev1_minutes, a.games as f_prev1_games,
    a.goals_p90 as f_prev1_goals_p90, a.xg_p90 as f_prev1_xg_p90,
    a.assists_p90 as f_prev1_assists_p90, a.xa_p90 as f_prev1_xa_p90,
    a.shots_p90 as f_prev1_shots_p90, a.key_passes_p90 as f_prev1_key_passes_p90,
    b.minutes as f_prev2_minutes, b.goals_p90 as f_prev2_goals_p90, b.xg_p90 as f_prev2_xg_p90,
    (a.player_key is not null)::int                    as f_has_prev1,

    -- ===== nhãn: kết quả mùa này (SAU) =====
    p.minutes_played                                   as y_minutes,
    p.games                                            as y_games,
    p.goals                                            as y_goals,
    p.assists                                          as y_assists,
    p.xg                                               as y_xg,
    p.xa                                               as y_xa,
    p.goals_per90                                      as y_goals_per90,
    p.xg_per90                                         as y_xg_per90,
    p.xa_per90                                         as y_xa_per90,
    (p.season < {{ season_of('current_date') }})       as season_complete,
    (p.season < {{ season_of('current_date') }} and p.minutes_played >= 900) as is_reliable_target
from p
left join dp on dp.player_key = p.player_key
left join {{ ref('dim_competition') }} dc on dc.competition_key = p.competition_key
left join tot90 a on a.player_key = p.player_key and a.season = p.season - 1
left join tot90 b on b.player_key = p.player_key and b.season = p.season - 2
