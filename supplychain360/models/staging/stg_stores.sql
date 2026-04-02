WITH source AS (
    SELECT * FROM {{ source('sheets', 'SHEET1') }}
),

cleaned AS (
    SELECT
        STORE_ID                                AS store_id,
        TRIM(STORE_NAME)                        AS store_name,
        TRIM(CITY)                              AS city,
        TRIM(UPPER(STATE))                      AS state,
        TRIM(UPPER(REGION))                     AS region,
        TRY_TO_DATE(STORE_OPEN_DATE)            AS store_open_date,
        CURRENT_TIMESTAMP()                     AS ingested_at
    FROM source
    WHERE STORE_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY STORE_ID ORDER BY STORE_ID) = 1
)

SELECT * FROM cleaned
