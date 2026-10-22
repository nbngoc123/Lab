{{ config(tags=['ml_monthly']) }}

with s as (
    select * from {{ ref('mart_ml_training_snapshot') }}
),
w as (
    select * from {{ ref('mart_ml_training_window') }}
),
expected_splits as (
    select unnest(['train', 'valid', 'test']) as split
),
violations as (
    select 'wrong_split_or_label' as violation, cast(match_key as varchar) as detail
    from s
    where split is null or split not in ('train', 'valid', 'test')
       or not coalesce(is_finished and is_labeled and is_warm, false)
       or y_result_code is null or y_result_code not in (0, 1, 2)
       or match_date is null or match_date > training_data_as_of
       or split <> case
           when match_date <= training_train_end then 'train'
           when match_date <= training_valid_end then 'valid'
           else 'test' end
    union all
    select 'label_not_available_at_cutoff', cast(match_key as varchar)
    from s
    where training_label_known_ts is null or training_label_cutoff_ts is null
       or training_label_known_ts >= training_label_cutoff_ts
       or training_label_cutoff_ts <> case split
           when 'train' then cast(training_valid_start as timestamp)
           when 'valid' then cast(training_test_start as timestamp)
           else cast(training_data_as_of as timestamp) + interval 1 day end
    union all
    select 'snapshot_window_mismatch', cast(s.match_key as varchar)
    from s cross join w
    where s.training_window_id is distinct from w.window_id
       or s.training_data_as_of is distinct from w.data_as_of
       or s.training_train_end is distinct from w.train_end
       or s.training_valid_start is distinct from w.valid_start
       or s.training_valid_end is distinct from w.valid_end
       or s.training_test_start is distinct from w.test_start
    union all
    select 'duplicate_match_key', cast(match_key as varchar)
    from s group by match_key having count(*) > 1
    union all
    select 'empty_split', e.split
    from expected_splits e left join s on s.split = e.split
    group by e.split having count(s.match_key) = 0
    union all
    select 'train_missing_class', cast(c.label as varchar)
    from (select unnest([0, 1, 2]) as label) c
    left join s on s.split = 'train' and s.y_result_code = c.label
    group by c.label having count(s.match_key) = 0
)
select * from violations
