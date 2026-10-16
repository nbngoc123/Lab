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
    coalesce(c.competition_name, p.competition_key)                AS competition_name,
    c.tier                                                          AS competition_tier,
    p.home_key,
    coalesce(ht.team_name, p.home_key)                              AS home_team_name,
    ht.short_name                                                   AS home_team_short,
    ht.logo_url                                                     AS home_logo_url,
    p.away_key,
    coalesce(awt.team_name, p.away_key)                             AS away_team_name,
    awt.short_name                                                   AS away_team_short,
    awt.logo_url                                                     AS away_logo_url,
    concat(coalesce(ht.team_name, p.home_key), ' vs ', coalesce(awt.team_name, p.away_key)) AS match_fixture,
    p.split,
    p.is_finished,
    p.actual_result,
    
    -- ===== PHÂN LOẠI TIMELINE THEO NGÀY HÔM NAY (CURRENT_DATE) =====
    (not coalesce(p.is_finished, false) and cast(p.match_date as date) >= current_date) AS is_upcoming_future,
    (cast(p.match_date as date) - current_date)                    AS days_to_kickoff,
    case
        when cast(p.match_date as date) = current_date and not coalesce(p.is_finished, false) then '1. Hôm nay'
        when cast(p.match_date as date) = current_date + 1 and not coalesce(p.is_finished, false) then '2. Ngày mai'
        when cast(p.match_date as date) > current_date and cast(p.match_date as date) <= current_date + 7 and not coalesce(p.is_finished, false) then '3. Trong 7 ngày tới'
        when cast(p.match_date as date) > current_date + 7 and not coalesce(p.is_finished, false) then '4. Tương lai xa hơn'
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
LEFT JOIN dwh.dim_team awt ON awt.team_key = p.away_key;


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
-- Kết hợp cả Thống kê Offline Benchmark (Train/Val/Test) và Thống kê Live Production Inference
DROP VIEW IF EXISTS dwh.v_model_performance_kpi CASCADE;
CREATE OR REPLACE VIEW dwh.v_model_performance_kpi AS
SELECT
    m.model_version,
    m.model_name,
    m.algorithm,
    m.experiment_name,
    m.run_id,
    m.split_method,
    m.dataset_source,
    m.n_raw_samples,
    m.n_filtered_samples,
    m.n_train_samples,
    m.n_validation_samples,
    m.n_test_samples,
    m.n_features,
    m.train_date_range,
    m.valid_date_range,
    m.test_date_range,
    m.learning_rate,
    m.n_estimators,
    m.draw_weight,
    m.training_duration_sec,
    -- Metrics đánh giá Offline Benchmark
    m.test_accuracy_pct,
    m.valid_accuracy_pct,
    m.test_balanced_acc_pct,
    m.test_macro_f1_pct,
    m.test_weighted_f1_pct,
    m.test_log_loss,
    m.test_mcc,
    m.test_home_precision_pct,
    m.test_home_recall_pct,
    m.test_home_f1_pct,
    m.test_draw_precision_pct,
    m.test_draw_recall_pct,
    m.test_draw_f1_pct,
    m.test_away_precision_pct,
    m.test_away_recall_pct,
    m.test_away_f1_pct,
    -- Metrics suy luận Live Production
    p.live_upcoming_matches,
    p.live_pred_home,
    p.live_pred_draw,
    p.live_pred_away,
    p.live_avg_confidence_pct,
    p.last_inference_time
FROM dwh.dim_ml_model_metadata m
LEFT JOIN (
    SELECT
        model_version,
        count(*) AS live_upcoming_matches,
        count(CASE WHEN predicted_result_letter = 'H' THEN 1 END) AS live_pred_home,
        count(CASE WHEN predicted_result_letter = 'D' THEN 1 END) AS live_pred_draw,
        count(CASE WHEN predicted_result_letter = 'A' THEN 1 END) AS live_pred_away,
        round(cast(avg(confidence) * 100 as numeric), 2) AS live_avg_confidence_pct,
        max(predicted_at) AS last_inference_time
    FROM dwh.mart_ml_predictions
    GROUP BY model_version
) p ON p.model_version = m.model_version;


-- 5. VIEW BẢNG XẾP HẠNG ĐẶC TRƯNG QUAN TRỌNG (GLOBAL FEATURE IMPORTANCE)
-- Phục vụ biểu đồ Bar Chart xếp hạng mức độ đóng góp toàn cục của 92 đặc trưng
DROP VIEW IF EXISTS dwh.v_model_feature_importance CASCADE;
CREATE OR REPLACE VIEW dwh.v_model_feature_importance AS
SELECT
    rank,
    feature_name,
    coalesce(feature_label, feature_name) as feature_label,
    importance_score,
    relative_pct
FROM dwh.mart_ml_feature_importance
ORDER BY rank ASC;


-- 6. VIEW ĐÁNH GIÁ CHI TIẾT THEO TỪNG LỚP (PER-CLASS PRECISION / RECALL / F1)
-- Phục vụ biểu đồ Radar hoặc Bar so sánh hiệu năng giữa tập Test và Validation
DROP VIEW IF EXISTS dwh.v_model_class_metrics CASCADE;
CREATE OR REPLACE VIEW dwh.v_model_class_metrics AS
SELECT *
FROM dwh.mart_ml_model_class_metrics
ORDER BY split DESC, class_name ASC;

