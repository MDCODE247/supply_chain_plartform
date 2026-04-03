{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
    {%- if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {%- endif %}
),

stores AS (
    SELECT * FROM {{ ref('dim_stores') }}
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    s.transaction_id,
    s.transaction_date,
    s.transaction_timestamp,
    s.source_table,
    s.store_id,
    st.store_name,
    st.city                     AS store_city,
    st.state                    AS store_state,
    st.region                   AS store_region,
    s.product_id,
    p.product_name,
    p.category                  AS product_category,
    p.brand,
    s.quantity_sold,
    s.unit_price,
    s.discount_pct,
    s.sale_amount,
    ROUND(s.sale_amount * (1 - s.discount_pct / 100), 2) AS net_sale_amount,
    s.ingested_at
FROM sales s
LEFT JOIN stores st ON s.store_id = st.store_id
LEFT JOIN products p ON s.product_id = p.product_id
