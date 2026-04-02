WITH source AS (
    SELECT * FROM {{ source('raw', 'SUPPLIERS') }}
),

cleaned AS (
    SELECT
        SUPPLIER_ID                             AS supplier_id,
        TRIM(SUPPLIER_NAME)                     AS supplier_name,
        TRIM(UPPER(CATEGORY))                   AS category,
        TRIM(UPPER(COUNTRY))                    AS country,
        CURRENT_TIMESTAMP()                     AS ingested_at
    FROM source
    WHERE SUPPLIER_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SUPPLIER_ID ORDER BY SUPPLIER_ID) = 1
)

SELECT * FROM cleaned
