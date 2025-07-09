{{
    config(
        materialized='table'
    )
}}
{% set source_model = ref ('raw', 'project') %}
{% set target_model = 'stg_valid_customers' %}
{% set error_table  = 'INSURANCE_DB.
DBT_PH_STAGING' %}
{% set unique_check_columns = ['project_id'] %}

{{ validate_and_route(
    source_model=source_model,
    target_model=target_model,
    error_table=error_table,
    unique_check_columns=unique_check_columns
) }}


