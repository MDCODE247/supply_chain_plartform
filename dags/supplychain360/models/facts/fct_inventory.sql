WITH inventory AS (
    SELECT * FROM {{ ref('stg_inventory') }}
),

warehouses AS (
    SELECT * FROM {{ ref('dim_warehouses') }}
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    i.snapshot_date,
    i.warehouse_id,
    w.city                      AS warehouse_city,
    w.state                     AS warehouse_state,
    i.product_id,
    p.product_name,
    p.category                  AS product_category,
    p.brand,
    p.supplier_id,
    p.supplier_name,
    i.quantity_available,
    i.reorder_threshold,
    i.is_below_threshold,
    ROUND(i.quantity_available / NULLIF(i.reorder_threshold, 0), 2) AS stock_ratio,
    i.ingested_at
FROM inventory i
LEFT JOIN warehouses w ON i.warehouse_id = w.warehouse_id
LEFT JOIN products p ON i.product_id = p.product_id
