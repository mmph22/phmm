{% macro insert_record_count_recon(model, compare_model, source_custom_query,target_custom_query, where_condition={}, count_multiplier={}) %}
{% do log("insert_record_count macro running...", info=True) %}
{% set select_sql%}
    {{ record_count_reconiliation(model, compare_model, source_custom_query,target_custom_query, where_condition={}, count_multiplier={}) }}
{% endset %}
{% set result = run_query(select_sql) %}
    {% for row in result %}
        {% set source_tbl_name = row[0] %}
        {% set target_tbl_name = row[1] %}
        {% set status = row[2] %}
        {% do log("source_tbl_name: {} | target_tbl_name: {} | status: {}".format(source_tbl_name,target_tbl_name,status), info=True) %}  
        {%set insert_sql %}
                INSERT INTO {{ env_var('DBT_AUDIT_DB') }}.{{ env_var('DBT_AUDIT_SCHEMA') }}.{{ env_var('DBT_ROW_CNT_RECON_TBL') }} (
                source_tbl_name,
                target_tbl_name,
                failure_error,
                status,
                LAST_MODIFIED_USER_NAME,
                LAST_MODIFIED_TIME
                )
                values(
                '{{ source_tbl_name }}',
                '{{ target_tbl_name }}',
                '{{ status }}',
                'Failed',
                CURRENT_USER(),
                CURRENT_TIMESTAMP()
            )
        {% endset%}
        {% do run_query(insert_sql) %}
        {% do log("inserted data into insert_record_count reconfiliation table completed.", info=True) %}
        {{
                raise_user_defined_exception("source_tbl_name: {} | target_tbl_name: {} | status: {} not matching".format(source_tbl_name,target_tbl_name,status))
        }}      
    {% endfor %}

{% endmacro %}
