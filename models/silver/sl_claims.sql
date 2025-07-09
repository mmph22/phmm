{{ config(
    materialized='table',
    pre_hook="{% set status = insert_data_into_audit_table(
        model.config.ops_ins,
        model.name,
        model.config.src_name,
        model.config.status_start,
        model.config.proc_typ_msg_start,
        model.config.stage,
        model.config.integration_id,
        model.config.flag
    ) %}",
    post_hook=[
        "{% set status = insert_data_into_audit_table(
            model.config.ops_upd,
            model.name,
            model.config.src_name,
            model.config.status_success,
            model.config.proc_typ_msg_success,
            model.config.stage,
            model.config.integration_id,
            '') %}",

        "{{ insert_record_count_recon(
            model.name,
            'CLAIMS',
            '{}.{}_{}.{}'.format(
                env_var('DBT_AUDIT_DB'),
                'dbt_ph',
                env_var('DBT_SL_SCHEMA'),
                model.name
            ),
            '{}.{}_{}.{}'.format(
                env_var('DBT_AUDIT_DB'),
                'dbt_ph',
                env_var('DBT_RAW_SCHEMA'),
                'RAW_CLAIMS'
            ),
            {'base': 'CLAIM_STATUS = ''Approved'''}, 
            {'base': 1, 'target': 1}
        ) }}"
    ]
) }}


{% set pk_column = 'claim_id' %}
{% set raw_table = ref('raw_claims') %}

with filtered as (
    {{ exclude_failed_rows(raw_table, pk_column) }}
)

select * from filtered
