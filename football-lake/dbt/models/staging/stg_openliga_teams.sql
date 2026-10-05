{{ config(materialized='view') }}

select
    {{ jget('a', '$.team_id', 'int') }}  as team_id,
    {{ jget('a', '$.team_name') }}       as team_name,
    {{ jget('a', '$.short_name') }}      as short_name,
    {{ jget('a', '$.icon_url') }}        as icon_url
from {{ cdc_current('teams', 'team_id') }} as c
