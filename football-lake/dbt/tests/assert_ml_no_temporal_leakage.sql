-- depends_on: {{ ref('mart_ml_match_features') }}
-- depends_on: {{ ref('mart_ml_team_match_features') }}
{# KIỂM THỬ CHỐNG RÒ RỈ THỜI GIAN (TEMPORAL LEAKAGE TRIPWIRE)
   Test này kiểm tra 3 điều kiện tiên quyết:
   1. Phân chia Train / Valid / Test theo ngày lịch tuyệt đối: không có bất kỳ ngày (match_date) nào
      bị phân mảnh cắt ngang 2 tập khác nhau.
   2. Tính toàn vẹn 1-to-1 của JOIN: mỗi match_key trong mart_ml_match_features chỉ có đúng 1 dòng,
      và trong mart_ml_team_match_features chỉ có đúng 1 dòng 'h' và 1 dòng 'a'.
   3. Tính an toàn của trận đầu tiên: không có trận nào có f_n_prev < 0.
#}

with split_overlap as (
    -- Kiểm tra 1: Không được có ngày nào bị chia thành 2 split
    select
        'split_date_overlap' as violation_type,
        cast(match_date as varchar) as identifier,
        count(distinct split) as metric_value
    from {{ ref('mart_ml_match_features') }}
    where split in ('train', 'valid', 'test')
    group by match_date
    having count(distinct split) > 1
),

match_dup as (
    -- Kiểm tra 2: mart_ml_match_features không được có match_key trùng lặp
    select
        'duplicate_match_key' as violation_type,
        match_key as identifier,
        count(*) as metric_value
    from {{ ref('mart_ml_match_features') }}
    group by match_key
    having count(*) > 1
),

team_match_dup as (
    -- Kiểm tra 3: mart_ml_team_match_features mỗi (match_key, side) chỉ có đúng 1 dòng
    select
        'duplicate_team_side' as violation_type,
        match_key || '_' || side as identifier,
        count(*) as metric_value
    from {{ ref('mart_ml_team_match_features') }}
    group by match_key, side
    having count(*) > 1
),

invalid_prev as (
    -- Kiểm tra 4: f_n_prev không được âm
    select
        'negative_f_n_prev' as violation_type,
        team_match_key as identifier,
        f_n_prev as metric_value
    from {{ ref('mart_ml_team_match_features') }}
    where f_n_prev < 0
)

select * from split_overlap
union all
select * from match_dup
union all
select * from team_match_dup
union all
select * from invalid_prev
