{{ config(materialized='external', tags=['ml_monthly']) }}

-- One configuration row for the entire training snapshot.
-- The default is the last completed UTC calendar month of this dbt invocation.
-- Pass ml_training_as_of_date explicitly when rebuilding a historical window.
with parameters as (
    select
        timestamp '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as generated_at,
        cast(nullif('{{ var("ml_training_as_of_date", "") }}', '') as date) as requested_as_of,
        cast('{{ var("ml_training_valid_months", 1) }}' as integer) as valid_months,
        cast('{{ var("ml_training_test_months", 1) }}' as integer) as test_months
),
cutoff as (
    select *, coalesce(
        requested_as_of,
        cast(date_trunc('month', generated_at) - interval 1 day as date)
    ) as data_as_of
    from parameters
),
test_boundary as (
    select *, cast(
        date_trunc('month', data_as_of) - (test_months - 1) * interval 1 month
        as date
    ) as test_start
    from cutoff
),
boundaries as (
    select *,
        cast(test_start - interval 1 day as date) as valid_end,
        cast(test_start - valid_months * interval 1 month as date) as valid_start
    from test_boundary
)
select
    md5('monthly_v1|' || cast(data_as_of as varchar) || '|' ||
        cast(valid_months as varchar) || '|' || cast(test_months as varchar)) as window_id,
    'monthly_v1' as split_policy,
    data_as_of,
    cast(valid_start - interval 1 day as date) as train_end,
    valid_start,
    valid_end,
    test_start,
    valid_months,
    test_months,
    generated_at
from boundaries
