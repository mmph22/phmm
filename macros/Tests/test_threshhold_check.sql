{% test test_threshold_check(model) %}

  {% set compare_model_name = kwargs.get("compare_model_name", kwargs.get("arg")) %}
  {% set column = kwargs.get("column", kwargs.get("arg")) %}
  {% set operators = kwargs.get("operators", kwargs.get("arg")) %}
  {% set filter = kwargs.get("filter", kwargs.get("arg")) %}
  {% set additional_filter = kwargs.get("additional_filter", kwargs.get("arg")) %}
  {% set v_cfg_tbl_name = "adt_fpa_config" %}
  {% set v_fq_cfg_tbl_name = env_var('DBT_AUDIT_DB') ~ '.' ~ env_var('DBT_AUDIT_SCHEMA') ~ '.' ~ v_cfg_tbl_name %}
  {% set final_query = namespace(query="") %}
    -- depends_on: {{ ref(compare_model_name) }}

  {% if execute %}
    {%- call statement("get_threshold_val", fetch_result=True) -%}
    select Threshold from {{v_fq_cfg_tbl_name}} WHERE Flag='Y' and Source= '{{model.name}}'
    {%- endcall -%}
    {%- set threshold_lst = load_result("get_threshold_val")["data"] -%}
    {% if threshold_lst|length > 0 %}
      {%- set threshold_Per = threshold_lst[0][0] -%}
    {% else %}
      {{exceptions.raise_compiler_error("Threshold value missing from the config table")}}
    {% endif %}

    with
    {%- for key, value in additional_filter.items() -%}
      {% set source_where_builder = ( column.source_column ~ " " ~ operators.source_operators ~ " " ~ (filter.source_filter | replace("@key", key)) ) %}
      {% set target_where_builder = ( column.target_column ~ " " ~ operators.target_operators ~ " " ~ (filter.target_filter | replace("@value", value)) ) %}

      base_model_{{ key }} as (
        select count(*) as count_a,'{{value}}' as source_name
        from {{ model }}
        where {{ source_where_builder }}
      ),
      compare_model_{{ key }} as (
        select count(*) as count_b,'{{value}}' as source_name
        from {{ ref(compare_model_name) }}
        where {{ target_where_builder }}
      ),
      final_{{ key }} as (
        select (
          case when compare_model_{{ key }}.count_b = 0 then 0
               else 100 * ( ( base_model_{{ key }}.count_a / compare_model_{{ key }}.count_b ) - 1 )
          end
        )::number(38, 2) as threshold_per,
        base_model_{{ key }}.source_name
        from base_model_{{ key }}, compare_model_{{ key }}
      ) {{ "," if not loop.last }}

      {% set query %}
        (select * from final_{{key}} where threshold_per <= -{{threshold_Per}} or threshold_per >={{threshold_Per}})
      {% endset %}

      {% set final_query.query %}
        {{final_query.query}} {{query}} {{ "union all" if not loop.last }}
      {% endset %}
    {%- endfor -%}

    {{ final_query.query }}
  {% endif %}
{% endtest %}