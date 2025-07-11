{% test assert_column_not_null(model, column_name) %}
    SELECT * FROM {{ model }} WHERE {{ column_name }} IS NULL
{% endtest %}

{% test assert_column_unique(model, column_name) %}
    SELECT * FROM {{ model }} M
    WHERE {{ column_name }} IN 
            (SELECT {{ column_name }} FROM {{ model }} 
                GROUP BY {{ column_name }} HAVING COUNT(*) > 1)
{% endtest %}

{% test expect_column_values_to_be_in_list(model, column_name, values) %}
    SELECT * FROM {{ model }} WHERE {{ column_name }} NOT IN (
        {% for val in values %}'{{ val }}'{% if not loop.last %}, {% endif %}{% endfor %}
    )
{% endtest %}


{% test validate_foreign_key(model, column_name, ref_model, ref_column) %}
    SELECT * FROM {{ model }} M
    WHERE {{ column_name }} IN (SELECT {{ column_name }}
                                    FROM {{ model }} m
                                    WHERE NOT EXISTS(
                                        SELECT 1
                                        FROM {{ ref_model }} rm
                                        WHERE rm.{{ ref_column }}= m.{{ column_name }}
                                    ))
{% endtest %}