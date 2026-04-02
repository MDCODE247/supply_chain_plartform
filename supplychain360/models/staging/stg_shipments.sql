WITH source AS (
    SELECT * FROM {{ source('raw', 'SHIPMENTS') }}
),

cleaned AS (
    SELECT
        SHIPMENT_ID                                         AS shipment_id,
        WAREHOUSE_ID                                        AS warehouse_id,
        STORE_ID                                            AS store_id,
        PRODUCT_ID                                          AS product_id,
        CAST(QUANTITY_SHIPPED AS INTEGER)                   AS quantity_shipped,
        TRY_TO_DATE(SHIPMENT_DATE)                          AS shipment_date,
        TRY_TO_DATE(EXPECTED_DELIVERY_DATE)                 AS expected_delivery_date,
        TRY_TO_DATE(ACTUAL_DELIVERY_DATE)                   AS actual_delivery_date,
        TRIM(UPPER(CARRIER))                                AS carrier,
        DATEDIFF('day',
            TRY_TO_DATE(EXPECTED_DELIVERY_DATE),
            TRY_TO_DATE(ACTUAL_DELIVERY_DATE))              AS delivery_delay_days,
        CASE
            WHEN TRY_TO_DATE(ACTUAL_DELIVERY_DATE) > TRY_TO_DATE(EXPECTED_DELIVERY_DATE)
            THEN TRUE ELSE FALSE
        END                                                 AS is_late,
        CURRENT_TIMESTAMP()                                 AS ingested_at
    FROM source
    WHERE SHIPMENT_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SHIPMENT_ID ORDER BY SHIPMENT_ID) = 1
)

SELECT * FROM cleaned
