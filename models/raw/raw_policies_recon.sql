{{
    config(
        materialized='table'
    )
}}
select * from {{ ref('policies_seed_raw') }}