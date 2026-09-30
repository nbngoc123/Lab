{{ config(materialized='view') }}
{# 1 cầu thủ có thể có nhiều quốc tịch / vị trí => nhiều dòng (grain: player_qid + country + position). #}

select distinct
    {{ wd_qid('binding_obj', 'player') }}          as player_qid,
    {{ wd('binding_obj', 'playerLabel') }}         as player_name,
    {{ wd('binding_obj', 'clubLabel') }}           as club_name,
    {{ wd_date('binding_obj', 'dob') }}            as date_of_birth,
    {{ wd('binding_obj', 'height', 'double') }}    as height_cm,
    {{ wd('binding_obj', 'countryLabel') }}        as country,
    {{ wd('binding_obj', 'positionLabel') }}       as position,
    {{ wd('binding_obj', 'sexLabel') }}            as sex,
    ingest_date
from {{ ref('stg_wikidata') }}
where sparql_query_name = 'pl_players'
