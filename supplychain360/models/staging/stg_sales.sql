WITH source AS (
    SELECT * FROM {{ source('supabase', 'SALES') }}
),

cleaned AS (
    SELECT
        TRANSACTION_ID                                                              AS transaction_id,
        STORE_ID                                                                    AS store_id,
        PRODUCT_ID                                                                  AS product_id,
        CAST(QUANTITY_SOLD AS INTEGER)                                              AS quantity_sold,
        CAST(UNIT_PRICE AS FLOAT)                                                   AS unit_price,
        CAST(DISCOUNT_PCT AS FLOAT)                                                 AS discount_pct,
        CAST(SALE_AMOUNT AS FLOAT)                                                  AS sale_amount,
        TO_TIMESTAMP(CAST(TRANSACTION_TIMESTAMP AS BIGINT) / 1000000000)            AS transaction_timestamp,
        DATE(TO_TIMESTAMP(CAST(TRANSACTION_TIMESTAMP AS BIGINT) / 1000000000))      AS transaction_date,
        CURRENT_TIMESTAMP()                                                         AS ingested_at
    FROM source
    WHERE TRANSACTION_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRANSACTION_ID ORDER BY TRANSACTION_TIMESTAMP) = 1
)

SELECT * FROM cleaned
