{# ===== Helper macros cho tầng core ===== #}

{# Tên đội (bất kỳ nguồn) -> team_key chuẩn theo seed team_alias.
   Không map được thì giữ nguyên tên gốc (đã trim) để dòng KHÔNG bị mất;
   model audit_unmapped_team_names liệt kê các tên này để bổ sung vào seed. #}
{% macro team_key(col) -%}
coalesce(
    (select a.team_key from {{ ref('int_team_alias') }} a where a.alias_lower = lower(trim({{ col }}))),
    nullif(trim({{ col }}), '')
)
{%- endmacro %}

{# Tên đội có nằm trong seed không (để gắn cờ chất lượng) #}
{% macro team_mapped(col) -%}
exists (select 1 from {{ ref('int_team_alias') }} a where a.alias_lower = lower(trim({{ col }})))
{%- endmacro %}

{# Chuẩn hóa tên người: bỏ dấu, chữ thường, chỉ giữ a-z và khoảng trắng #}
{% macro name_norm(col) -%}
trim(regexp_replace(regexp_replace(lower(strip_accents({{ col }})), '[^a-z ]', ' ', 'g'), '\s+', ' ', 'g'))
{%- endmacro %}

{# Từ cuối của tên đã chuẩn hóa (họ) #}
{% macro last_word(col) -%}
list_extract(string_split({{ name_norm(col) }}, ' '), -1)
{%- endmacro %}

{# Mùa giải theo năm bắt đầu: tháng >= 7 thuộc mùa bắt đầu năm đó #}
{% macro season_of(date_col) -%}
case when month({{ date_col }}) >= 7 then year({{ date_col }}) else year({{ date_col }}) - 1 end
{%- endmacro %}

{# Như team_key nhưng KHÔNG fallback: không map được thì NULL (dùng cho tên sân vận động) #}
{% macro team_key_strict(col) -%}
(select a.team_key from {{ ref('int_team_alias') }} a where a.alias_lower = lower(trim({{ col }})))
{%- endmacro %}

{# Khóa trận tự nhiên: cùng 1 trận ở mọi nguồn => cùng khóa (đội nhà, đội khách, ngày) #}
{% macro match_key(home, away, date) -%}
md5(lower({{ home }}) || '|' || lower({{ away }}) || '|' || cast({{ date }} as varchar))
{%- endmacro %}
