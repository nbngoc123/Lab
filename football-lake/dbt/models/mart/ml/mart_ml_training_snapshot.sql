{{ config(materialized='external', tags=['ml_monthly']) }}

-- Training-only export. The existing feature mart keeps upcoming fixtures and
-- its historical benchmark split, so inference and dashboard schemas stay intact.
with assigned as (
    select
        m.* exclude (split),
        case
            when m.match_date <= w.train_end then 'train'
            when m.match_date <= w.valid_end then 'valid'
            else 'test'
        end as split,
        w.window_id as training_window_id,
        w.split_policy as training_split_policy,
        w.data_as_of as training_data_as_of,
        w.train_end as training_train_end,
        w.valid_start as training_valid_start,
        w.valid_end as training_valid_end,
        w.test_start as training_test_start,
        w.valid_months as training_valid_months,
        w.test_months as training_test_months,
        w.generated_at as training_generated_at,
        case
            when m.kickoff_utc is not null then
                cast(m.kickoff_utc as timestamp) + interval 3 hour
            else cast(m.match_date as timestamp) + interval 23 hour + interval 59 minute + interval 59 second
        end as training_label_known_ts
    from {{ ref('mart_ml_match_features') }} m
    cross join {{ ref('mart_ml_training_window') }} w
    where coalesce(m.is_finished, false)
      and coalesce(m.is_labeled, false)
      and coalesce(m.is_warm, false)
      and m.y_result_code in (0, 1, 2)
      and m.match_date <= w.data_as_of
),
availability as (
    select *,
        case split
            when 'train' then cast(training_valid_start as timestamp)
            when 'valid' then cast(training_test_start as timestamp)
            else cast(training_data_as_of as timestamp) + interval 1 day
        end as training_label_cutoff_ts
    from assigned
)
select * from availability
where training_label_known_ts < training_label_cutoff_ts
