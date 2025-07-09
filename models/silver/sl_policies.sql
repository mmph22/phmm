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

select
  policy_id,
  customer_id,
  policy_type,
  start_date,
  end_date,
  premium_amount
from {{ ref('raw_policies') }}
