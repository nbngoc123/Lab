{# ===== Helper macros cho lớp staging (DuckDB + MinIO/S3) ===== #}

{# Đường dẫn S3. Đổi bucket bằng: --vars '{lake_bucket: my-bucket}' #}
{% macro lake_path(suffix) -%}
's3://{{ var("lake_bucket", "football-lake") }}/{{ suffix }}'
{%- endmacro %}

{# Lấy giá trị hive-style từ cột filename: .../league=EPL/... -> 'EPL'.
   Dùng regex thay vì hive_partitioning=true để không đụng tên cột trong JSON. #}
{% macro path_part(key, col='filename') -%}
nullif(regexp_extract({{ col }}, '{{ key }}=([^/]+)', 1), '')
{%- endmacro %}

{# Ngày snapshot từ path (chịu được cả ingest_date=2026-09-30T13) #}
{% macro path_date(key='ingest_date', col='filename') -%}
try_cast(left(nullif(regexp_extract({{ col }}, '{{ key }}=([^/]+)', 1), ''), 10) as date)
{%- endmacro %}

{# Lấy 1 giá trị scalar từ cột JSON. try_cast => dòng lỗi thành NULL, để test not_null bắt. #}
{% macro jget(col, path, type='varchar') -%}
try_cast(json_extract_string({{ col }}, '{{ path }}') as {{ type }})
{%- endmacro %}

{# Timestamp ISO-8601 UTC ("...Z" hoặc "+00:00") -> TIMESTAMP #}
{% macro jts(col, path) -%}
try_cast(regexp_replace(json_extract_string({{ col }}, '{{ path }}'), '(Z|[+-]00:00)$', '') as timestamp)
{%- endmacro %}

{# Bung 1 mảng JSON thành nhiều dòng. Thiếu key / body lỗi => 0 dòng, không vỡ model. #}
{% macro jarray(col, path) -%}
unnest(from_json(json_extract({{ col }}, '{{ path }}'), '["JSON"]'))
{%- endmacro %}

{# ---- Wikidata SPARQL: mỗi biến có dạng {"type":..,"value":..} ---- #}
{% macro wd(col, field, type='varchar') -%}
try_cast(json_extract_string({{ col }}, '$.{{ field }}.value') as {{ type }})
{%- endmacro %}

{% macro wd_qid(col, field) -%}
nullif(regexp_extract(json_extract_string({{ col }}, '$.{{ field }}.value'), '(Q[0-9]+)$', 1), '')
{%- endmacro %}

{# coord là WKT "Point(lon lat)"; idx=1 -> lon, idx=2 -> lat #}
{% macro wd_coord(col, field, idx) -%}
try_cast(regexp_extract(json_extract_string({{ col }}, '$.{{ field }}.value'), 'Point\(([-0-9.eE+]+) ([-0-9.eE+]+)\)', {{ idx }}) as double)
{%- endmacro %}

{% macro wd_date(col, field) -%}
try_cast(left(json_extract_string({{ col }}, '$.{{ field }}.value'), 10) as date)
{%- endmacro %}
