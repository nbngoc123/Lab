
with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/thesportsdb/metadata/entity=players/**/*.json.gz') }}
),
items as (select filename, {{ jarray('doc', '$.player') }} as p from raw)

select
    {{ jget('p', '$.idPlayer') }}            as player_id,
    {{ jget('p', '$.idTeam') }}              as team_id,
    {{ jget('p', '$.strPlayer') }}           as player_name,
    {{ jget('p', '$.strNationality') }}      as nationality,
    {{ jget('p', '$.dateBorn', 'date') }}    as birth_date,
    {{ jget('p', '$.strWage') }}             as wage,
    {{ jget('p', '$.dateSigned', 'date') }}  as date_signed,
    {{ jget('p', '$.strSigning') }}          as signing_fee,
    {{ jget('p', '$.strOutfitter') }}        as outfitter,
    {{ jget('p', '$.strAgent') }}            as agent,
    {{ jget('p', '$.strPosition') }}         as position,
    {{ jget('p', '$.strHeight') }}           as height,
    {{ jget('p', '$.strWeight') }}           as weight,
    {{ jget('p', '$.idWikidata') }}          as wikidata_id,
    {{ jget('p', '$.idTransferMkt') }}       as transfermarkt_id,
    {{ jget('p', '$.idESPN') }}              as espn_id,
    {{ path_date() }}                        as ingest_date
from items
where json_extract_string(p, '$.idPlayer') is not null
qualify row_number() over (partition by player_id order by ingest_date desc nulls last, filename desc) = 1
