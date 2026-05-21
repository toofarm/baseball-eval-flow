{% macro json_get(column, key) -%}
{%- if target.type == 'snowflake' -%}
{{ column }}:{{ key }}
{%- else -%}
({{ column }}->>'{{ key }}')
{%- endif -%}
{%- endmacro %}


{% macro regex_match(column, pattern) -%}
{%- if target.type == 'snowflake' -%}
regexp_like({{ column }}, '{{ pattern }}')
{%- else -%}
{{ column }} ~ '{{ pattern }}'
{%- endif -%}
{%- endmacro %}
