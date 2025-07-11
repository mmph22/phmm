 -- depends_on: {{ ref('raw_claims') }}

{% set pk_column = 'claim_id' %}
{% set raw_table = ref('raw_claims') %}
{% set batch_filter = "batch_id = (select coalesce(max(batch_id) + 1, 1) from " ~ this ~ ")" %}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=pk_column,
    on_schema_change='sync_all_columns',

    pre_hook=[
        "{{ insert_data_into_audit_table(
            model.config.ops_ins,
            model.name,
            model.config.src_name,
            model.config.status_start,
            model.config.proc_typ_msg_start,
            model.config.stage,
            model.config.integration_id,
            model.config.flag
        ) }}"
    ],

    post_hook=[
        "{{ insert_data_into_audit_table(
            model.config.ops_upd,
            model.name,
            model.config.src_name,
            model.config.status_success,
            model.config.proc_typ_msg_success,
            model.config.stage,
            model.config.integration_id,
            ''
        ) }}",

        "{{ insert_record_count_recon(
            model.name,
            'CLAIMS',
            '{}.{}_{}.{}'.format(env_var('DBT_AUDIT_DB'), 'DBT_PH', env_var('DBT_SL_SCHEMA'), model.name),
            '{}.{}_{}.{}'.format(env_var('DBT_AUDIT_DB'), 'DBT_PH', env_var('DBT_RAW_SCHEMA'), 'RAW_CLAIMS'),
            where_condition={
                'base': batch_filter,
                'target': batch_filter
            }
        ) }}"
    ]
) }}

with max_batch as (
    {% if is_incremental() %}
        select coalesce(max(batch_id) , 0) as batch_id_value from {{ this }}
    {% else %}
        select 1 as batch_id_value
    {% endif %}
),

filtered as (
    {{ exclude_failed_rows(raw_table, pk_column) }}
),

with_batch_id as (
    select f.*
    from filtered f
    cross join max_batch m
    {% if is_incremental() %}
        where f.batch_id > m.batch_id_value
    {% endif %}
)

select * from with_batch_id
