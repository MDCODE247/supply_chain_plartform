SELECT
    warehouse_id,
    city,
    state,
    ingested_at
FROM {{ ref('stg_warehouses') }}
