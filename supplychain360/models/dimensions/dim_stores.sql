SELECT
    store_id,
    store_name,
    city,
    state,
    region,
    store_open_date,
    ingested_at
FROM {{ ref('stg_stores') }}
