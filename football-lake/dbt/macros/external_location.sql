{# Ghi đè macro mặc định của dbt-duckdb: nơi lưu Parquet của model `external`.
     staging  -> s3://<bucket>/dwh/staging/<tên>.parquet   (mỗi nguồn staging ghi file riêng của nó)
     còn lại  -> s3://<bucket>/dwh/<tên>.parquet           (core: giữ nguyên vị trí cũ)
   register_upstream_external_models() (on-run-start) cũng gọi macro này nên core đọc đúng file staging.
   Lưu ý: cấu hình `+external_location`/`{name}` KHÔNG có tác dụng trong dbt-duckdb (khóa đúng là `location`,
   và không hỗ trợ placeholder {name}) nên dùng macro này để đặt tên theo từng model. #}
{%- macro external_location(relation, config) -%}
  {%- set fmt = config.get('format', 'parquet') -%}
  {%- set sub = 'staging/' if relation.schema.endswith('staging') else '' -%}
  {{- adapter.external_root() }}/{{ sub }}{{ relation.identifier }}.{{ fmt }}
{%- endmacro -%}
