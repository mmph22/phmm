{% macro exclude_failed_rows(source_relation, pk_column) %}
    {% set source_table_name = source_relation.identifier %}
    {% set source_schema_name = source_relation.schema %}
    {% set full_table_name = source_relation.database ~ '.' ~ source_schema_name ~ '.' ~ source_table_name %}
    {% set audit_db = env_var('DBT_AUDIT_DB') %}
    {% set audit_schema = env_var('DBT_AUDIT_SCHEMA') %}
    {% set pk_col_upper = pk_column | upper %}

    {% do log("Running exclude_failed_rows for table: " ~ full_table_name, info=True) %}
    {% do log("Using PK column: " ~ pk_column, info=True) %}
    {% do log("Audit schema: " ~ audit_schema, info=True) %}

    {% set failed_rows_cte %}
        SELECT DISTINCT
            failure_data:{{ pk_col_upper }}::STRING AS failed_pk
        FROM {{ audit_db }}.{{ audit_schema }}.FAILED_TEST_RECORDS
        WHERE table_name ILIKE '{{ full_table_name }}'
    {% endset %}

    {% set final_query %}
        WITH failed_rows AS (
            {{ failed_rows_cte }}
        )
        SELECT * FROM {{ source_relation }} base
        WHERE CAST({{ pk_column }} AS STRING) NOT IN (SELECT failed_pk FROM failed_rows)
    {% endset %}

    {{ return(final_query) }}
{% endmacro %}
