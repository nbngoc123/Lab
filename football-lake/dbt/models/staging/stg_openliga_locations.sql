{{ config(materialized='view') }}

select
    {{ jget('a', '$.location_id', 'int') }}  as location_id,
    {{ jget('a', '$.city') }}                as city,
    {{ jget('a', '$.stadium') }}             as stadium
from {{ cdc_current('locations', 'location_id') }} as c
