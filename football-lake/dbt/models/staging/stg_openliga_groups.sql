{{ config(materialized='view') }}
{# Spieltag / Vorrunde / Finale ... #}

select
    {{ jget('a', '$.group_id', 'int') }}     as group_id,
    {{ jget('a', '$.league_id', 'int') }}    as league_id,
    {{ jget('a', '$.group_order', 'int') }}  as group_order,
    {{ jget('a', '$.group_name') }}          as group_name
from {{ cdc_current('groups', 'group_id') }} as c
