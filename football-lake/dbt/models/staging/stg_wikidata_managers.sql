{{ config(materialized='view') }}

select distinct
    {{ wd_qid('binding_obj', 'club') }}            as club_qid,
    {{ wd('binding_obj', 'clubLabel') }}           as club_name,
    {{ wd_qid('binding_obj', 'coach') }}           as coach_qid,
    {{ wd('binding_obj', 'coachLabel') }}          as coach_name,
    {{ wd_date('binding_obj', 'coachDob') }}       as coach_date_of_birth,
    {{ wd('binding_obj', 'coachCountryLabel') }}   as coach_country,
    ingest_date
from {{ ref('stg_wikidata') }}
where sparql_query_name = 'pl_managers'
