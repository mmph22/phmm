-- SELECT * FROM {{ source('raw', 'claims') }}

{{ 
  config( 
    materialized='incremental', 
    unique_key='claim_id'  
  ) 
}}

SELECT *, {{ incremental_hash('claims') }} 
FROM {{ source('raw', 'claims') }}
