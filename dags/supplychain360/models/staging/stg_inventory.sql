WITH source AS (
    SELECT * FROM {{ source('raw', 'INVENTORY') }}
),

cleaned AS (
    SELECT
        WAREHOUSE_ID                            AS warehouse_id,
        PRODUCT_ID                              AS product_id,
        CAST(QUANTITY_AVAILABLE AS INTEGER)     AS quantity_available,
        CAST(REORDER_THRESHOLD AS INTEGER)      AS reorder_threshold,
        TRY_TO_DATE(SNAPSHOT_DATE)              AS snapshot_date,
        CASE 
            WHEN CAST(QUANTITY_AVAILABLE AS INTEGER) <= CAST(REORDER_THRESHOLD AS INTEGER) 
            THEN TRUE ELSE FALSE 
        END                                     AS is_below_threshold,
        CURRENT_TIMESTAMP()                     AS ingested_at
    FROM source
    WHERE WAREHOUSE_ID IS NOT NULL AND PRODUCT_ID IS NOT NULL
)

SELECT * FROM cleaned
