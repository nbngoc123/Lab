{ config(
    materialized='view'
) }

with raw_data as (
    select *
    from read_json_auto('s3://football-lake/raw/wikipedia/**/*.json.gz')
)

select *
from raw_data
