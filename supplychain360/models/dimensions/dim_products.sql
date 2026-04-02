WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.unit_price,
    s.supplier_id,
    s.supplier_name,
    s.country                   AS supplier_country,
    p.ingested_at
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.supplier_id
