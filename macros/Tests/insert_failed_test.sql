{% macro insert_failed_tests(results) %}
  {# Select both failed and warning test results #}
  {% set issues = results | selectattr('status', 'in', ['fail', 'warn']) | list %}

  {% if issues | length > 0 %}
    {% do log("Found " ~ issues | length ~ " failed/warning tests. Inserting into FAILED_TEST_RECORDS...", info=True) %}

    {% for result in issues %}
      {% set node = result.node %}
      {% set test_name = node.name %}
      {% set error_type = node.test_metadata.name if node.test_metadata is defined else 'unknown' %}
      {% set relation = node.relation_name %}

      {% if relation is not none %}
        {% do log("Inserting from: " ~ relation ~ " | Test: " ~ test_name ~ " | Type: " ~ error_type, info=True) %}

        INSERT INTO {{ env_var('DBT_AUDIT_DB') }}.{{ env_var('DBT_AUDIT_SCHEMA') }}.FAILED_TEST_RECORDS (
          error_des,
          error_type,
          failure_data,
          inserted_at
        )
        SELECT
          '{{ test_name }}',
          '{{ error_type }}',
          TO_VARIANT(OBJECT_CONSTRUCT(*)),
          CURRENT_TIMESTAMP
        FROM {{ relation }};

      {% else %}
        {% do log("Skipping test (no relation): " ~ test_name, info=True) %}
      {% endif %}
    {% endfor %}

    {% do log("insert_failed_tests macro completed.", info=True) %}
  {% else %}
    {% do log("No failed or warning tests to insert.", info=True) %}
  {% endif %}
{% endmacro %}
