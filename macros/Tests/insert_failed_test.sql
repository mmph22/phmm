{% macro insert_failed_tests(results) %}
  {% set issues = results | selectattr('status', 'in', ['fail', 'warn']) | list %}

  {% if issues | length > 0 %}
    {% do log("Found " ~ issues | length ~ " failed/warning tests. Inserting into FAILED_TEST_RECORDS...", info=True) %}

    {% for result in issues %}
      {% set node = result.node %}
      {% set test_name = node.name %}
      {% set error_type = node.test_metadata.name if node.test_metadata is defined else 'unknown' %}
      {% set failure_relation = node.relation_name %}
      {% do log("failure_relation: " ~ failure_relation, info=True) %}
      {% do log("test_name: " ~ test_name, info=True) %}
      {% set test_column = (
          node.test_metadata.kwargs['column_name']
          if node.test_metadata is defined
          and node.test_metadata.kwargs is defined
          and 'column_name' in node.test_metadata.kwargs
          else 'unknown'
      ) %}
      {% do log("Test column: " ~ test_column, info=True) %}

      {# Get original model from dependencies #}
      {% set model_node_ref = node.depends_on.nodes[0] if node.depends_on.nodes | length > 0 else none %}
      {% set model_relation = graph.nodes[model_node_ref].relation_name if model_node_ref is not none and graph.nodes[model_node_ref] is defined else none %}
      {% do log("model_relation: " ~ model_relation, info=True) %}

      {% if failure_relation is not none and model_relation is not none %}
        {% do log("Processing test: " ~ test_name, info=True) %}

        {# Extract schema and table name from model_relation string #}
        {% set model_parts = model_relation.split('.') %}
        {% set model_schema = model_parts[1] if model_parts | length > 1 else '' %}
        {% set model_table = model_parts[2] if model_parts | length > 2 else '' %}
        {% do log("model_schema: " ~ model_schema, info=True) %}
        {% do log("model_table: " ~ model_table, info=True) %}

        {# Extract schema and table name from failure_relation string #}
        {% set failure_parts = failure_relation.split('.') %}
        {% set failure_schema = failure_parts[1] if failure_parts | length > 1 else '' %}
        {% set failure_table = failure_parts[2] if failure_parts | length > 1 else '' %}
        {% do log("failure_schema: " ~ failure_schema, info=True) %}
        {% do log("failure_table: " ~ failure_table, info=True) %}

        {% set failure_schema_upper = failure_schema | upper %}
        {% set failure_table_upper = failure_table | upper %}

        {% set columns_query %}
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{{ failure_schema_upper }}'
            AND table_name = '{{ failure_table_upper }}'
        {% endset %}
        {% set column_result = run_query(columns_query) %}
        {% set failure_column = column_result.columns[0].values()[0] if column_result and column_result.columns[0].values() | length > 0 else none %}

        {% if failure_column is not none %}
          {% do log("Failure column: " ~ failure_column, info=True) %}

          INSERT INTO {{ env_var('DBT_AUDIT_DB') }}.{{ env_var('DBT_AUDIT_SCHEMA') }}.FAILED_TEST_RECORDS (
            table_name,
            column_name,
            error_des,
            error_type,
            failure_data,
            inserted_at
          )
          SELECT
            '{{ model_relation }}',
            '{{ test_column }}',
            '{{ test_name }}',
            '{{ error_type }}',
            TO_VARIANT(OBJECT_CONSTRUCT(model.*)),
            CURRENT_TIMESTAMP
          FROM {{ model_relation }} AS model
          INNER JOIN {{ failure_relation }} AS failure
            ON model.{{ test_column }} = failure.{{ failure_column }};

        {% else %}
          {% do log("Could not determine failure column for test: " ~ test_name, info=True) %}
        {% endif %}

      {% else %}
        {% do log("Skipping test (missing failure or model relation): " ~ test_name, info=True) %}
      {% endif %}
    {% endfor %}

    {% do log("insert_failed_tests macro completed.", info=True) %}
  {% else %}
    {% do log("No failed or warning tests to insert.", info=True) %}
  {% endif %}
{% endmacro %}
