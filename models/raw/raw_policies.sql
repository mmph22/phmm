{{ 
  config( 
    materialized='incremental', 
    unique_key='policy_id'  
  ) 
}}


-- SELECT * FROM {{ source('raw', 'policies') }}

SELECT *, {{ incremental_hash('policies') }} 
FROM {{ source('raw', 'policies') }}
