{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
    {% if is_incremental() %}
    WHERE shipment_date > (SELECT MAX(shipment_date) FROM {{ this }})
    {% endif %}
),

warehouses AS (
    SELECT * FROM {{ ref('dim_warehouses') }}
),

stores AS (
    SELECT * FROM {{ ref('dim_stores') }}
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    sh.shipment_id,
    sh.shipment_date,
    sh.expected_delivery_date,
    sh.actual_delivery_date,
    sh.warehouse_id,
    w.city                      AS warehouse_city,
    w.state                     AS warehouse_state,
    sh.store_id,
    st.store_name,
    st.region                   AS store_region,
    sh.product_id,
    p.product_name,
    p.category                  AS product_category,
    sh.quantity_shipped,
    sh.carrier,
    sh.delivery_delay_days,
    sh.is_late,
    sh.ingested_at
FROM shipments sh
LEFT JOIN warehouses w ON sh.warehouse_id = w.warehouse_id
LEFT JOIN stores st ON sh.store_id = st.store_id
LEFT JOIN products p ON sh.product_id = p.product_id
