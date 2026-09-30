{ config(
    materialized='view'
) }

with raw_data as (
    select *
    from read_json_auto('s3://football-lake/raw/open_meteo/**/*.json.gz')
)

select *
from raw_data
