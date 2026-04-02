WITH source AS (
    SELECT * FROM {{ source('raw', 'PRODUCTS') }}
),

cleaned AS (
    SELECT
        PRODUCT_ID                              AS product_id,
        TRIM(PRODUCT_NAME)                      AS product_name,
        TRIM(UPPER(CATEGORY))                   AS category,
        TRIM(UPPER(BRAND))                      AS brand,
        SUPPLIER_ID                             AS supplier_id,
        CAST(UNIT_PRICE AS FLOAT)               AS unit_price,
        CURRENT_TIMESTAMP()                     AS ingested_at
    FROM source
    WHERE PRODUCT_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PRODUCT_ID ORDER BY PRODUCT_ID) = 1
)

SELECT * FROM cleaned
