{# ===== Helper cho tầng mart ML =====
   QUY ƯỚC CỘT (bắt buộc, để không rò rỉ nhãn khi train):
     f_*  = đặc trưng, CHỈ dùng thông tin biết TRƯỚC giờ đá (đã kiểm tra bằng test không-rò-rỉ).
     y_*  = nhãn / số liệu SAU trận (không bao giờ dùng làm đặc trưng).
     còn lại = khoá, metadata, cờ chất lượng (is_warm, split, has_*...). #}

{# Trung bình trượt chỉ khi đủ n quan sát không-NULL trong cửa sổ w (tránh trung bình của 1-2 trận đầu mùa) #}
{% macro roll_avg(expr, w, n) -%}
case when count({{ expr }}) over {{ w }} >= {{ n }} then avg({{ expr }}) over {{ w }} end
{%- endmacro %}

{# Danh sách đặc trưng số của MỘT đội (tên cột trong mart_ml_team_match_features, đã có tiền tố f_).
   mart_ml_match_features nhân mỗi cột thành f_home_*, f_away_*, f_diff_*. #}
{% macro ml_team_numeric_features() -%}
{{ return([
  'n_prev',
  'pts_l3', 'gf_l3', 'ga_l3',
  'pts_l5', 'gf_l5', 'ga_l5', 'gd_l5', 'win_rate_l5', 'cs_rate_l5', 'btts_rate_l5', 'over25_rate_l5',
  'xgf_l5', 'xga_l5', 'shots_l5', 'sot_l5', 'corners_l5', 'fouls_l5', 'yellows_l5', 'reds_l5',
  'ppda_l5', 'deep_l5', 'deep_allowed_l5', 'npxg_l5', 'npxga_l5',
  'pts_l10', 'gf_l10', 'ga_l10', 'xgf_l10', 'xga_l10', 'win_rate_l10',
  'pts_l20', 'gd_l20',
  'side_pts_l5', 'side_gf_l5', 'side_ga_l5',
  's_played', 's_ppg', 's_gf_pg', 's_ga_pg', 's_gd_pg',
  'h2h_n', 'h2h_pts', 'h2h_gf', 'h2h_ga',
  'rest_days', 'n_7d', 'n_14d', 'n_30d'
]) }}
{%- endmacro %}

{# Xác suất ngầm định từ tỉ lệ cược thập phân, chuẩn hoá bỏ biên nhà cái. i = tỉ lệ của kết cục cần tính, a,b = 2 kết cục còn lại #}
{% macro implied_prob(i, a, b) -%}
case when {{ i }} > 1 and {{ a }} > 1 and {{ b }} > 1
     then (1.0 / {{ i }}) / (1.0 / {{ i }} + 1.0 / {{ a }} + 1.0 / {{ b }}) end
{%- endmacro %}
