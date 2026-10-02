{{ config(materialized='table') }}

with d as (
    select unnest(generate_series(date '2020-01-01', date '2028-12-31', interval 1 day))::date as date_key
)
select
    date_key,
    year(date_key)                                   as year,
    month(date_key)                                  as month,
    day(date_key)                                    as day,
    isodow(date_key)                                 as iso_day_of_week,   -- 1=Thứ hai ... 7=Chủ nhật
    strftime(date_key, '%A')                         as day_name,
    weekofyear(date_key)                             as iso_week,
    quarter(date_key)                                as quarter,
    isodow(date_key) >= 6                            as is_weekend,
    {{ season_of('date_key') }}                      as season,            -- năm bắt đầu mùa (2024 = 2024/25)
    {{ season_of('date_key') }} || '/' || right(cast({{ season_of('date_key') }} + 1 as varchar), 2) as season_label
from d
