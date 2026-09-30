{ config(
    materialized='view'
) }

with raw_data as (
    select *
    from read_csv_auto('s3://football-lake/raw/football_data_couk/**/*.csv')
)

select *
from raw_data
