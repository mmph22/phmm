{{ 
  config(
    materialized='incremental',
    unique_key='policy_id',
    merge_exclude_columns=['policy_id'],
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
    post_hook=["{% set status = insert_data_into_audit_table(
        model.config.ops_upd,
        model.name,
        model.config.src_name,
        model.config.status_success,
        model.config.proc_typ_msg_success,
        model.config.stage,
        model.config.integration_id,
        ''
    ) %}"]
) }}
{% set hash_expr = incremental_hash('raw_policies', exclude_columns=['created_at', 'updated_at']) %}
{% set cleaned_hash = hash_expr | replace('as hash_val', '') %}
{{ log("Updated hash expression: " ~ cleaned_hash, info=True) }}
SELECT *
FROM {{ ref('raw_policies') }}
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }}
    WHERE {{ this }}.policy_id = {{ ref('raw_policies') }}.policy_id
      AND {{ this }}.hash_val = {{ cleaned_hash }}
)
{% endif %}
