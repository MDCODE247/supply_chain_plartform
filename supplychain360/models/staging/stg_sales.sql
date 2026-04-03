WITH sales_union AS (
    {% set sales_tables = [
        'sales_2026_03_10', 'sales_2026_03_11', 'sales_2026_03_12',
        'sales_2026_03_13', 'sales_2026_03_14', 'sales_2026_03_15',
        'sales_2026_03_16'
    ] %}

    {% for table in sales_tables %}
    SELECT
        TRANSACTION_ID,
        STORE_ID,
        PRODUCT_ID,
        CAST(QUANTITY_SOLD AS INTEGER)                                          AS quantity_sold,
        CAST(UNIT_PRICE AS FLOAT)                                               AS unit_price,
        CAST(DISCOUNT_PCT AS FLOAT)                                             AS discount_pct,
        CAST(SALE_AMOUNT AS FLOAT)                                              AS sale_amount,
        TO_TIMESTAMP(CAST(TRANSACTION_TIMESTAMP AS BIGINT) / 1000000000)        AS transaction_timestamp,
        DATE(TO_TIMESTAMP(CAST(TRANSACTION_TIMESTAMP AS BIGINT) / 1000000000))  AS transaction_date,
        '{{ table }}'                                                           AS source_table,
        CURRENT_TIMESTAMP()                                                     AS ingested_at
    FROM {{ source('supabase', table) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

cleaned AS (
    SELECT *
    FROM sales_union
    WHERE TRANSACTION_ID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRANSACTION_ID ORDER BY TRANSACTION_TIMESTAMP) = 1
)

SELECT * FROM cleaned
