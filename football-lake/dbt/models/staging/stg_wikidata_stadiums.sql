{# venue_lat/venue_lon parse từ WKT "Point(lon lat)": nguồn cho p13 (Open-Meteo) thay vì tự dựng silver/dim/wd_stadiums. #}

select distinct
    {{ wd_qid('binding_obj', 'venue') }}         as venue_qid,
    {{ wd('binding_obj', 'venueLabel') }}        as venue_name,
    {{ wd('binding_obj', 'capacity', 'int') }}   as capacity,
    {{ wd_coord('binding_obj', 'coord', 2) }}    as lat,
    {{ wd_coord('binding_obj', 'coord', 1) }}    as lon,
    {{ wd_date('binding_obj', 'opened') }}       as opened_date,
    {{ wd('binding_obj', 'cityLabel') }}         as city,
    {{ wd('binding_obj', 'countryLabel') }}      as country,
    ingest_date
from {{ ref('stg_wikidata') }}
where sparql_query_name = 'pl_stadiums'
