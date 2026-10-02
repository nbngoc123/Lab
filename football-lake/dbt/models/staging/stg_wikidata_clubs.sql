
select distinct
    {{ wd_qid('binding_obj', 'club') }}      as club_qid,
    {{ wd('binding_obj', 'clubLabel') }}     as club_name,
    {{ wd_date('binding_obj', 'inception') }} as inception_date,
    {{ wd_qid('binding_obj', 'venue') }}     as venue_qid,
    {{ wd('binding_obj', 'venueLabel') }}    as venue_name,
    {{ wd('binding_obj', 'capacity', 'int') }} as venue_capacity,
    {{ wd_coord('binding_obj', 'coord', 2) }} as venue_lat,
    {{ wd_coord('binding_obj', 'coord', 1) }} as venue_lon,
    {{ wd('binding_obj', 'coachLabel') }}    as coach_name,
    {{ wd('binding_obj', 'websiteURL') }}    as website_url,
    ingest_date
from {{ ref('stg_wikidata') }}
where sparql_query_name = 'pl_clubs'
