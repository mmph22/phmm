{{ config(
    materialized='view',
    pre_hook="{% set status = insert_data_into_audit_table(
        model.config.ops_ins,
        model.name,
        model.config.src_name,
        model.config.status_start,
        model.config.proc_typ_msg_start,
        model.config.stage,
        model.config.integration_id,
        model.config.flag
    ) %}",post_hook=["{% set status = insert_data_into_audit_table(
        model.config.ops_upd,
        model.name,
        model.config.src_name,      
        model.config.status_success,
        model.config.proc_typ_msg_success,
        model.config.stage,
        model.config.integration_id,
        '') %}"]
) }}

SELECT
  p.policy_id,
  p.customer_id,
  p.policy_type,
  p.start_date,
  p.end_date,
  p.premium_amount,
  c.claim_id,
  c.claim_date,
  c.claim_amount,
  c.claim_status
FROM {{ ref('sl_policies') }} p
LEFT JOIN {{ ref('sl_claims') }} c
  ON p.policy_id = c.policy_id
