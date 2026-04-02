WITH source AS (
    SELECT * FROM {{ source('raw', 'WAREHOUSES') }}
),

cleaned AS (
    SELECT
        WAREHOUSE_ID                            AS warehouse_id,
        TRIM(CITY)                              AS city,
        TRIM(UPPER(STATE))                      AS state,
        CURRENT_TIMESTAMP()                     AS ingested_at
    FROM source
    WHERE WAREHOUSE_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY WAREHOUSE_ID ORDER BY WAREHOUSE_ID) = 1
)

SELECT * FROM cleaned
