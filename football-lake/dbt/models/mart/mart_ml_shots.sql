{{ config(materialized='external') }}
{# ML mart: 1 dòng / CÚ SÚT (Understat). Bài toán: tự huấn luyện mô hình xG (y_is_goal), phân loại cú sút, game-state.
   - f_* : đặc trưng biết tại thời điểm sút (toạ độ, khoảng cách/góc sút, tình huống, kiểu sút, phút, tỉ số TRƯỚC phút đó).
   - benchmark_xg = xG của Understat (đầu ra của MÔ HÌNH KHÁC): dùng để so sánh, KHÔNG đưa vào đặc trưng.
   - Toạ độ Understat chuẩn hoá 0..1, khung thành ở x=1; sân 105x68m. Tỉ số trước sút chỉ tính các bàn 'Goal' ở phút TRƯỚC
     (bàn cùng phút và phản lưới bị bỏ qua có chủ ý để không suy diễn sai quy ước). #}

with s as (
    select *,
        (1 - x_coord) * 105.0 as dx,
        (0.5 - y_coord) * 68.0 as dy
    from {{ ref('stg_understat_shots') }}
),
st as (
    select shot_id,
        coalesce(sum((side = 'h' and result = 'Goal')::int) over wp, 0) as home_goals_before,
        coalesce(sum((side = 'a' and result = 'Goal')::int) over wp, 0) as away_goals_before
    from s
    window wp as (partition by match_id order by minute range between unbounded preceding and 1 preceding)
)
select
    s.shot_id,
    cast(s.match_id as varchar)                                   as understat_match_id,
    fm.match_key,
    coalesce(dc.competition_key, s.league)                        as competition_key,
    s.season,
    {{ team_key('s.team_name') }}                                 as team_key,
    (s.side = 'h')                                                as is_home,
    pk.player_key,
    s.player_name,

    -- ===== đặc trưng =====
    s.minute                                                      as f_minute,
    s.x_coord                                                     as f_x,
    s.y_coord                                                     as f_y,
    sqrt(s.dx * s.dx + s.dy * s.dy)                               as f_distance_m,
    atan2(7.32 * s.dx, s.dx * s.dx + s.dy * s.dy - 3.66 * 3.66)  as f_angle_rad,
    s.situation                                                   as f_situation,
    s.shot_type                                                   as f_shot_type,
    s.last_action                                                 as f_last_action,
    (s.shot_type = 'Head')::int                                   as f_is_header,
    (s.situation = 'Penalty')::int                                as f_is_penalty,
    (s.assisted_by is not null)::int                              as f_is_assisted,
    case when s.side = 'h' then st.home_goals_before - st.away_goals_before
         else st.away_goals_before - st.home_goals_before end     as f_score_diff_before,

    -- ===== nhãn & benchmark =====
    (s.result = 'Goal')::int                                      as y_is_goal,
    s.result                                                      as y_result,
    s.xg                                                          as benchmark_xg
from s
join st on st.shot_id = s.shot_id
left join {{ ref('dim_player') }} pk on pk.understat_player_id = cast(s.player_id as varchar)
left join {{ ref('dim_competition') }} dc on dc.understat_league = s.league
left join {{ ref('fact_match') }} fm on fm.understat_match_id = cast(s.match_id as varchar)
