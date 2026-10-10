{{ config(materialized='external') }}
{# ML mart - bảng DÀI: 1 trận -> 2 dòng (đội nhà / đội khách), đặc trưng "phong độ" tính TẠI THỜI ĐIỂM TRƯỚC TRẬN.

   CHỐNG RÒ RỈ (thiết kế, không phải hy vọng):
   - Mọi cửa sổ trượt chỉ chạy trên các trận ĐÃ KẾT THÚC (played), sau đó gắn vào từng dòng bằng ASOF JOIN với điều kiện
     "trận đó kết thúc (giờ đá + 3h) TRƯỚC giờ đá của dòng hiện tại". Nên đặc trưng của một trận không thể chứa kết quả
     của chính nó, cũng không chứa trận hoãn/chưa đá nằm trước nó.
   - Trung bình trượt NULL nếu chưa đủ cửa sổ (đầu mùa) -> dùng f_n_prev để quyết định lọc.
   - f_* là đặc trưng (an toàn trước trận), y_* là nhãn của chính dòng này (SAU trận). #}

with base as (
    select
        t.team_match_key, t.match_key, t.competition_key, t.season, t.match_date,
        cast(t.kickoff_utc as timestamp)                                   as kickoff_utc,
        coalesce(cast(t.kickoff_utc as timestamp), cast(t.match_date as timestamp)) as ord_ts,
        t.side, t.team_key, t.opponent_key, t.is_finished,
        (t.is_finished and t.goals_for is not null and t.goals_against is not null) as played,
        t.goals_for, t.goals_against, t.points,
        t.xg_for, t.xg_against, t.shots, t.shots_target, t.corners, t.fouls, t.yellows, t.reds,
        t.ppda_att / nullif(t.ppda_def, 0)                                 as ppda,
        t.deep, t.deep_allowed, t.npxg, t.npxga
    from {{ ref('fact_team_match') }} t
    where t.team_key is not null
      and t.match_date >= date '{{ var('ml_min_match_date') }}'   -- bỏ ngày placeholder (vd 1970-01-01)
),

played as (
    select *,
        ord_ts + interval 3 hour                                           as known_ts,   -- thời điểm kết quả "đã biết"
        (points = 3)::int                                                  as win,
        (goals_against = 0)::int                                           as cs,
        (goals_for > 0 and goals_against > 0)::int                         as btts,
        (goals_for + goals_against > 2)::int                               as over25,
        goals_for - goals_against                                          as gd,
        case points when 3 then 'W' when 1 then 'D' else 'L' end           as res
    from base
    where played
),

-- ---------- phong độ chung (mọi sân, mọi giải) ----------
roll as (
    select
        team_key, known_ts, ord_ts as last_ord_ts,
        count(*) over wall                                                 as f_n_prev,
        {{ roll_avg('points', 'w3', 3) }}                                  as f_pts_l3,
        {{ roll_avg('goals_for', 'w3', 3) }}                               as f_gf_l3,
        {{ roll_avg('goals_against', 'w3', 3) }}                           as f_ga_l3,
        {{ roll_avg('points', 'w5', 5) }}                                  as f_pts_l5,
        {{ roll_avg('goals_for', 'w5', 5) }}                               as f_gf_l5,
        {{ roll_avg('goals_against', 'w5', 5) }}                           as f_ga_l5,
        {{ roll_avg('gd', 'w5', 5) }}                                      as f_gd_l5,
        {{ roll_avg('win', 'w5', 5) }}                                     as f_win_rate_l5,
        {{ roll_avg('cs', 'w5', 5) }}                                      as f_cs_rate_l5,
        {{ roll_avg('btts', 'w5', 5) }}                                    as f_btts_rate_l5,
        {{ roll_avg('over25', 'w5', 5) }}                                  as f_over25_rate_l5,
        {{ roll_avg('xg_for', 'w5', 5) }}                                  as f_xgf_l5,
        {{ roll_avg('xg_against', 'w5', 5) }}                              as f_xga_l5,
        {{ roll_avg('shots', 'w5', 5) }}                                   as f_shots_l5,
        {{ roll_avg('shots_target', 'w5', 5) }}                            as f_sot_l5,
        {{ roll_avg('corners', 'w5', 5) }}                                 as f_corners_l5,
        {{ roll_avg('fouls', 'w5', 5) }}                                   as f_fouls_l5,
        {{ roll_avg('yellows', 'w5', 5) }}                                 as f_yellows_l5,
        {{ roll_avg('reds', 'w5', 5) }}                                    as f_reds_l5,
        {{ roll_avg('ppda', 'w5', 5) }}                                    as f_ppda_l5,
        {{ roll_avg('deep', 'w5', 5) }}                                    as f_deep_l5,
        {{ roll_avg('deep_allowed', 'w5', 5) }}                            as f_deep_allowed_l5,
        {{ roll_avg('npxg', 'w5', 5) }}                                    as f_npxg_l5,
        {{ roll_avg('npxga', 'w5', 5) }}                                   as f_npxga_l5,
        {{ roll_avg('points', 'w10', 10) }}                                as f_pts_l10,
        {{ roll_avg('goals_for', 'w10', 10) }}                             as f_gf_l10,
        {{ roll_avg('goals_against', 'w10', 10) }}                         as f_ga_l10,
        {{ roll_avg('xg_for', 'w10', 10) }}                                as f_xgf_l10,
        {{ roll_avg('xg_against', 'w10', 10) }}                            as f_xga_l10,
        {{ roll_avg('win', 'w10', 10) }}                                   as f_win_rate_l10,
        {{ roll_avg('points', 'w20', 20) }}                                as f_pts_l20,
        {{ roll_avg('gd', 'w20', 20) }}                                    as f_gd_l20,
        case when count(res) over w5 >= 5 then string_agg(res, '') over w5 end as f_form5
    from played
    window
        wall as (partition by team_key order by ord_ts, match_key rows between unbounded preceding and current row),
        w3   as (partition by team_key order by ord_ts, match_key rows between 2  preceding and current row),
        w5   as (partition by team_key order by ord_ts, match_key rows between 4  preceding and current row),
        w10  as (partition by team_key order by ord_ts, match_key rows between 9  preceding and current row),
        w20  as (partition by team_key order by ord_ts, match_key rows between 19 preceding and current row)
),

-- ---------- phong độ theo sân (chỉ các trận cùng sân nhà/khách) ----------
roll_side as (
    select
        team_key, side, known_ts,
        {{ roll_avg('points', 'w5', 5) }}        as f_side_pts_l5,
        {{ roll_avg('goals_for', 'w5', 5) }}     as f_side_gf_l5,
        {{ roll_avg('goals_against', 'w5', 5) }} as f_side_ga_l5
    from played
    window w5 as (partition by team_key, side order by ord_ts, match_key rows between 4 preceding and current row)
),

-- ---------- tích luỹ trong mùa (theo giải) ----------
roll_season as (
    select
        team_key, competition_key, season, known_ts,
        count(*) over ws                                         as f_s_played,
        avg(points) over ws                                      as f_s_ppg,
        avg(goals_for) over ws                                   as f_s_gf_pg,
        avg(goals_against) over ws                               as f_s_ga_pg,
        avg(gd) over ws                                          as f_s_gd_pg
    from played
    window ws as (partition by team_key, competition_key, season order by ord_ts, match_key
                  rows between unbounded preceding and current row)
),

-- ---------- đối đầu trực tiếp (5 lần gần nhất, góc nhìn của team_key) ----------
roll_h2h as (
    select
        team_key, opponent_key, known_ts,
        count(*) over wh                                         as f_h2h_n,
        avg(points) over wh                                      as f_h2h_pts,
        avg(goals_for) over wh                                   as f_h2h_gf,
        avg(goals_against) over wh                               as f_h2h_ga
    from played
    window wh as (partition by team_key, opponent_key order by ord_ts, match_key rows between 4 preceding and current row)
),

-- ---------- mật độ thi đấu: số trận đã đá trong 7/14/30 ngày trước giờ đá ----------
congestion as (
    select
        b.team_match_key,
        count(*) filter (where p.ord_ts >= b.ord_ts - interval 7 day)  as f_n_7d,
        count(*) filter (where p.ord_ts >= b.ord_ts - interval 14 day) as f_n_14d,
        count(*)                                                       as f_n_30d
    from base b
    join played p
      on p.team_key = b.team_key
     and p.known_ts < b.ord_ts
     and p.ord_ts >= b.ord_ts - interval 30 day
    group by b.team_match_key
)

select
    b.team_match_key, b.match_key, b.competition_key, b.season, b.match_date, b.kickoff_utc,
    b.side, b.team_key, b.opponent_key, b.is_finished, b.played as is_labeled,

    -- ===== đặc trưng (an toàn trước trận) =====
    coalesce(r.f_n_prev, 0)                                                   as f_n_prev,
    r.f_pts_l3, r.f_gf_l3, r.f_ga_l3,
    r.f_pts_l5, r.f_gf_l5, r.f_ga_l5, r.f_gd_l5, r.f_win_rate_l5, r.f_cs_rate_l5, r.f_btts_rate_l5, r.f_over25_rate_l5,
    r.f_xgf_l5, r.f_xga_l5, r.f_shots_l5, r.f_sot_l5, r.f_corners_l5, r.f_fouls_l5, r.f_yellows_l5, r.f_reds_l5,
    r.f_ppda_l5, r.f_deep_l5, r.f_deep_allowed_l5, r.f_npxg_l5, r.f_npxga_l5,
    r.f_pts_l10, r.f_gf_l10, r.f_ga_l10, r.f_xgf_l10, r.f_xga_l10, r.f_win_rate_l10,
    r.f_pts_l20, r.f_gd_l20,
    sd.f_side_pts_l5, sd.f_side_gf_l5, sd.f_side_ga_l5,
    coalesce(se.f_s_played, 0)                                                as f_s_played,
    se.f_s_ppg, se.f_s_gf_pg, se.f_s_ga_pg, se.f_s_gd_pg,
    coalesce(h.f_h2h_n, 0)                                                    as f_h2h_n,
    h.f_h2h_pts, h.f_h2h_gf, h.f_h2h_ga,
    case when r.last_ord_ts is not null then epoch(b.ord_ts - r.last_ord_ts) / 86400.0 end as f_rest_days,
    coalesce(c.f_n_7d, 0)                                                     as f_n_7d,
    coalesce(c.f_n_14d, 0)                                                    as f_n_14d,
    coalesce(c.f_n_30d, 0)                                                    as f_n_30d,
    r.f_form5,

    -- ===== nhãn của chính dòng này (SAU trận) =====
    case when b.played then b.goals_for end                                   as y_goals_for,
    case when b.played then b.goals_against end                               as y_goals_against,
    case when b.played then b.points end                                      as y_points,
    case when b.played then b.xg_for end                                      as y_xg_for,
    case when b.played then b.xg_against end                                  as y_xg_against
from base b
asof left join roll        r  on r.team_key = b.team_key and b.ord_ts > r.known_ts
asof left join roll_side   sd on sd.team_key = b.team_key and sd.side = b.side and b.ord_ts > sd.known_ts
asof left join roll_season se on se.team_key = b.team_key and se.competition_key = b.competition_key
                             and se.season = b.season and b.ord_ts > se.known_ts
asof left join roll_h2h    h  on h.team_key = b.team_key and h.opponent_key = b.opponent_key and b.ord_ts > h.known_ts
left join congestion c on c.team_match_key = b.team_match_key
