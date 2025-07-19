{{
    config(
        materialized='table'
    )
}}
select * from {{ ref('claims_seed_raw') }}
