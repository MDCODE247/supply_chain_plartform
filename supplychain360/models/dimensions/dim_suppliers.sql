SELECT
    supplier_id,
    supplier_name,
    category,
    country,
    ingested_at
FROM {{ ref('stg_suppliers') }}
