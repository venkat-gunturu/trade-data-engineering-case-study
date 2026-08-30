{#
    Land each model in the exact schema named by +schema in dbt_project.yml.

    dbt's default implementation concatenates target.schema with the custom
    schema name, which would produce BRONZE_SILVER instead of SILVER. The
    repository convention (README section 3) requires the schemas to be named
    RAW / BRONZE / SILVER / GOLD literally, so the override is deliberate.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
