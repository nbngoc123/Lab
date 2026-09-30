{ config(
    materialized='view'
) }

with raw_data as (
    select *
    from read_json_auto('s3://football-lake/raw/google_news/**/*.json.gz')
)

select *
from raw_data
