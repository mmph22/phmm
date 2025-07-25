{{ config(
    materialized='incremental',
    post_hook=[
        "DELETE FROM {{ this }} WHERE batch_id = (SELECT max(batch_id) - 1 FROM {{ this }})",
    ]
) }}

SELECT 
    {% if is_incremental() %}
        (select coalesce(max(batch_id) + 1, 1) from {{ this }}) as batch_id,
    {% else %}
        1 as batch_id,
    {% endif %}
    *,
FROM {{ source('raw', 'claims') }}
{% if is_incremental() %}
    where last_updated > (select coalesce(max(last_updated), '1900-01-01') from {{ this }})
{% endif %}
