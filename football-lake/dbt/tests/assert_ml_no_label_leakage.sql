-- depends_on: {{ ref('mart_ml_match_features') }}
{# TRIPWIRE chống rò rỉ nhãn: mọi đặc trưng số f_* của mart_ml_match_features phải có |tương quan| với hiệu số bàn thắng (y_goal_diff)
   <= ml_leak_corr_max (mặc định 0.55) trên các trận đã đá & is_warm. Trên dữ liệu thật: đặc trưng hợp lệ mạnh nhất là xác suất
   nhà cái ~0.45, còn cột "forecast" Understat (thực chất tính từ xG của chính trận) ~0.64 => bị bắt. Chỉ đánh giá khi có >= 500
   trận (mẫu nhỏ cho tương quan ngẫu nhiên cao). Test FAIL nếu quét ít hơn 50 cột (tránh "pass" rỗng do lỗi suy kiểu cột). #}
{% if execute %}
    {% set cols = [] %}
    {% for col in adapter.get_columns_in_relation(ref('mart_ml_match_features')) %}
        {% if col.name.startswith('f_') and col.is_number() %}{% do cols.append(col.name) %}{% endif %}
    {% endfor %}
    {% if cols | length < 50 %}
        {{ exceptions.raise_compiler_error('assert_ml_no_label_leakage chỉ quét được ' ~ (cols | length) ~ ' cột f_ số (cần >= 50): kiểm tra suy kiểu cột') }}
    {% endif %}
    with lab as (
        select * from {{ ref('mart_ml_match_features') }} where is_labeled and is_warm
    ),
    c as (
        select {% for n in cols %}corr({{ n }}, y_goal_diff) as {{ n }}{{ ',' if not loop.last }}{% endfor %}
        from lab
        having count(*) >= 500
    ),
    u as (
        unpivot c on {{ cols | join(', ') }} into name feature value r
    )
    -- cột hằng số cho corr = NaN, mà DuckDB coi NaN lớn hơn mọi số => phải loại tường minh
    select feature, r from u where r is not null and not isnan(r) and abs(r) > {{ var('ml_leak_corr_max', 0.55) }}
{% else %}
    select 1 as feature, 1.0 as r where false
{% endif %}
