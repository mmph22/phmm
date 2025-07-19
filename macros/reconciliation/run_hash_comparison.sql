{% macro run_hash_comparison() %}
  {{ compare_table_hashes_and_aggregates_with_audit_log([
      ('raw_claims_recon', 'sl_claims_recon'),
      ('raw_policies_recon', 'sl_policies_recon')
    ],
    exclude_columns=['last_updated'],
    aggregation_columns={
      'raw_claims_recon': 'claim_amount',
      'sl_claims_recon': 'claim_amount',  
      'raw_policies_recon': 'premium_amount',
      'sl_policies_recon': 'premium_amount',  
    }
  ) }}
{% endmacro %}