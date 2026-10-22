{{ config(tags=['ml_monthly']) }}

with w as (
    select * from {{ ref('mart_ml_training_window') }}
),
violations as (
    select 'window_row_count' as violation, cast(count(*) as varchar) as detail
    from w having count(*) <> 1
    union all
    select 'invalid_window', window_id from w
    where valid_months < 1 or test_months < 1
       or data_as_of is null or train_end is null or valid_start is null
       or valid_end is null or test_start is null or generated_at is null
       or data_as_of >= cast(generated_at as date)
       or not (train_end < valid_start and valid_start <= valid_end
               and valid_end < test_start and test_start <= data_as_of)
       or train_end + interval 1 day <> valid_start
       or valid_end + interval 1 day <> test_start
)
select * from violations
