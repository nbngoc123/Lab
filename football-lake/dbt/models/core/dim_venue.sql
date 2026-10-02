{{ config(materialized='table') }}
{# Sân vận động từ Wikidata (có tọa độ). team_key lấy qua seed (alias nguồn 'openmeteo' = tên sân).
   Lưu ý: 1 sân chung 2 đội (San Siro...) chỉ gán được cho 1 đội theo seed. #}

with s as (
    select venue_qid, venue_name, capacity, lat, lon, opened_date, city, country
    from {{ ref('stg_wikidata_stadiums') }}
    where venue_qid is not null
),
agg as (
    select
        venue_qid,
        any_value(venue_name)   as venue_name,
        max(capacity)           as capacity,
        any_value(lat)          as lat,
        any_value(lon)          as lon,
        min(opened_date)        as opened_date,
        any_value(city)         as city,
        any_value(country)      as country
    from s group by venue_qid
)
select
    venue_qid,
    venue_name,
    {{ team_key_strict('venue_name') }}                         as team_key,
    capacity, lat, lon, opened_date, city, country,
    -- khóa thư mục mà p13 dùng khi ghi raw/open_meteo/.../venue=<venue_safe>/
    replace(replace(venue_name, ' ', '_'), '/', '_')            as openmeteo_venue_key
from agg
