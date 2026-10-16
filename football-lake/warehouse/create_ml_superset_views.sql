-- ==============================================================================
-- SQL VIEWS CHO DASHBOARD DỰ ĐOÁN KẾT QUẢ TRẬN ĐẤU & GIẢI THÍCH SHAP (SUPERSET)
-- Database: football_dwh | Schema: dwh
-- ==============================================================================

-- 1. VIEW CHÍNH: Toàn bộ danh sách trận đấu kèm phân loại timeline theo CURRENT_DATE
CREATE OR REPLACE VIEW dwh.v_match_predictions_dashboard AS
SELECT
    p.match_key,
    p.match_date,
    p.kickoff_utc,
    p.competition_key,
    coalesce(c.name, p.competition_key)                             AS competition_name,
    c.tier                                                          AS competition_tier,
    p.home_key,
    coalesce(ht.name, p.home_key)                                   AS home_team_name,
    ht.short_name                                                   AS home_team_short,
    ht.logo_url                                                     AS home_logo_url,
    p.away_key,
    coalesce(at.name, p.away_key)                                   AS away_team_name,
    at.short_name                                                   AS away_team_short,
    at.logo_url                                                     AS away_logo_url,
    concat(coalesce(ht.name, p.home_key), ' vs ', coalesce(at.name, p.away_key)) AS match_fixture,
    p.split,
    p.is_finished,
    p.actual_result,
    
    -- ===== PHÂN LOẠI TIMELINE THEO NGÀY HÔM NAY (CURRENT_DATE) =====
    (not coalesce(p.is_finished, false) and p.match_date >= current_date) AS is_upcoming_future,
    cast(p.match_date - current_date as integer)                   AS days_to_kickoff,
    case
        when p.match_date = current_date and not coalesce(p.is_finished, false) then '1. Hôm nay'
        when p.match_date = current_date + interval '1 day' and not coalesce(p.is_finished, false) then '2. Ngày mai'
        when p.match_date > current_date and p.match_date <= current_date + interval '7 days' and not coalesce(p.is_finished, false) then '3. Trong 7 ngày tới'
        when p.match_date > current_date + interval '7 days' and not coalesce(p.is_finished, false) then '4. Tương lai xa hơn'
        else '5. Đã kết thúc / Quá khứ'
    end                                                             AS fixture_timeline_group,

    p.p_home,
    p.p_draw,
    p.p_away,
    round(cast(p.p_home * 100 as numeric), 1)                       AS prob_home_pct,
    round(cast(p.p_draw * 100 as numeric), 1)                       AS prob_draw_pct,
    round(cast(p.p_away * 100 as numeric), 1)                       AS prob_away_pct,
    concat(
        'H: ', round(cast(p.p_home * 100 as numeric)), '% | ',
        'D: ', round(cast(p.p_draw * 100 as numeric)), '% | ',
        'A: ', round(cast(p.p_away * 100 as numeric)), '%'
    )                                                               AS prob_summary_bar,
    p.predicted_result,
    p.predicted_result_code,
    p.predicted_result_letter,
    p.confidence,
    round(cast(p.confidence * 100 as numeric), 1)                   AS confidence_pct,
    p.is_correct,
    case
        when p.is_correct = 1 then 'Chính xác'
        when p.is_correct = 0 then 'Chưa chính xác'
        else 'Chờ thi đấu'
    end                                                             AS prediction_status,
    p.top_driver_1_desc,
    p.top_driver_1_impact,
    p.top_driver_2_desc,
    p.top_driver_2_impact,
    p.top_driver_3_desc,
    p.top_driver_3_impact,
    p.risk_driver_1_desc,
    p.risk_driver_1_impact,
    p.model_version,
    p.predicted_at
FROM dwh.mart_ml_predictions p
LEFT JOIN dwh.dim_competition c ON c.competition_key = p.competition_key
LEFT JOIN dwh.dim_team ht ON ht.team_key = p.home_key
LEFT JOIN dwh.dim_team at ON at.team_key = p.away_key;


-- 2. VIEW CHUYÊN DỤNG: CÁC TRẬN ĐẤU SẮP DIỄN RA TỪ HÔM NAY TRỞ ĐI (UPCOMING FIXTURES)
-- Dành riêng cho bảng Dashboard "Lịch thi đấu & Dự đoán tương lai"
CREATE OR REPLACE VIEW dwh.v_upcoming_match_predictions AS
SELECT *
FROM dwh.v_match_predictions_dashboard
WHERE is_upcoming_future = true OR split = 'predict'
ORDER BY match_date ASC, kickoff_utc ASC;


-- 3. VIEW GIẢI THÍCH SHAP: Bảng dạng dài phục vụ biểu đồ Bar Chart / Waterfall theo trận
CREATE OR REPLACE VIEW dwh.v_match_shap_explainability AS
SELECT
    s.match_key,
    v.match_fixture,
    v.match_date,
    v.fixture_timeline_group,
    v.competition_name,
    v.home_team_name,
    v.away_team_name,
    s.predicted_result,
    v.confidence_pct,
    s.feature_name,
    s.feature_label,
    s.feature_value,
    s.shap_impact,
    abs(s.shap_impact)                                              AS shap_abs_impact,
    s.impact_direction,
    case
        when s.shap_impact > 0 then '#10B981'                       -- Màu xanh: Ủng hộ
        else '#EF4444'                                              -- Màu đỏ: Rủi ro / Cản trở
    end                                                             AS impact_color
FROM dwh.mart_ml_prediction_shap_factors s
JOIN dwh.v_match_predictions_dashboard v ON v.match_key = s.match_key;


-- 4. VIEW TỔNG HỢP HIỆU NĂNG MÔ HÌNH (MODEL PERFORMANCE KPI)
CREATE OR REPLACE VIEW dwh.v_model_performance_kpi AS
SELECT
    p.model_version,
    count(*)                                                        AS total_predictions,
    count(case when p.is_finished and p.actual_result is not null then 1 end) AS evaluated_matches,
    count(case when not p.is_finished or p.actual_result is null then 1 end)  AS upcoming_matches,
    count(case when not coalesce(p.is_finished, false) and p.match_date >= current_date then 1 end) AS upcoming_from_today,
    round(
        cast(avg(case when p.is_finished and p.actual_result is not null then p.is_correct end) * 100 as numeric),
        2
    )                                                               AS accuracy_pct,
    round(cast(avg(p.confidence) * 100 as numeric), 2)              AS avg_confidence_pct,
    count(case when p.predicted_result_letter = 'H' then 1 end)     AS count_pred_home,
    count(case when p.predicted_result_letter = 'D' then 1 end)     AS count_pred_draw,
    count(case when p.predicted_result_letter = 'A' then 1 end)     AS count_pred_away,
    max(p.predicted_at)                                             AS last_inference_time
FROM dwh.mart_ml_predictions p
GROUP BY p.model_version;
