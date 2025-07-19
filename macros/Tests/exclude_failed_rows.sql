{% macro exclude_failed_rows(source_relation, pk_column) %}
    {% set source_table_name = source_relation.identifier %}
    {% set audit_schema = target.schema ~ '_' ~ env_var('DBT_AUDIT_SCHEMA') %}
    {% do log("Running exclude_failed_rows for table: " ~ source_table_name, info=True) %}
    {% do log("Using PK column: " ~ pk_column, info=True) %}
    {% do log("Audit schema: " ~ audit_schema, info=True) %}

    {% set get_tables_query %}
        SELECT table_name
        FROM {{ target.database }}.information_schema.tables
        WHERE table_schema = '{{ audit_schema | upper }}'
          AND table_name ILIKE '%{{ source_table_name | upper }}%'
    {% endset %}

    {% set table_result = run_query(get_tables_query) %}
    {% set test_tables = table_result.columns[0].values() if table_result else [] %}

    {% if test_tables | length == 0 %}
        {% do log("No test tables found. Returning all rows", info=True) %}
        {{ return("SELECT * FROM " ~ source_relation) }}
    {% endif %}

    {% set union_queries = [] %}
    {% for table in test_tables %}
        {% set q = "SELECT DISTINCT " ~ pk_column ~ " AS pk FROM " ~ audit_schema ~ "." ~ table %}
        {% do union_queries.append(q) %}
    {% endfor %}

    {% set failed_cte = "(" ~ union_queries | join(" UNION ") ~ ")" %}
    {% set final_query %}
        SELECT * FROM {{ source_relation }} base
        WHERE {{ pk_column }} NOT IN (
            SELECT pk FROM {{ failed_cte }}
        )
    {% endset %}

    {{ return(final_query) }}
{% endmacro %}
