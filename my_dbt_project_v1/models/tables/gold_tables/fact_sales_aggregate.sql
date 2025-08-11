

WITH order_header AS (
    SELECT 
        SALES_ORDER_ID,
        ORDER_DATE,
        CUSTOMER_ID,
        SALES_PERSON_ID,
        TERRITORY,
        regexp_extract(TERRITORY, '\\((.*?)\\)', 1) AS COUNTRY,
        ACCOUNT_NUMBER,
        SHIP_METHOD,
        SUB_TOTAL,
        TAX_AMOUNT,
        FREIGHT,
        TOTAL_DUE
    FROM {{ ref('fact_sales_orders') }}
),

order_details_aggregated AS (
    SELECT 
        SALES_ORDER_ID,
        PRODUCT_ID,
        SUM(LINE_TOTAL) AS TOTAL_LINE_AMOUNT,
        SUM(ORDER_QUANTITY) AS TOTAL_QUANTITY, 
        SUM(UNIT_PRICE) AS TOTAL_PRICE,
        SUM(UNIT_PRICE_DISCOUNT) AS TOTAL_DISCOUNT
    FROM {{ ref('fact_sales_orders_details') }}
    GROUP BY SALES_ORDER_ID
    , PRODUCT_ID
)


SELECT 
    h.SALES_ORDER_ID,
    d.PRODUCT_ID,
    pr.NAME AS PRODUCT_NAME,
    p.FULL_NAME AS CUSTOMER_NAME,
    h.ORDER_DATE,
    h.CUSTOMER_ID,
    h.SALES_PERSON_ID,
    s.STORE_ID,
    h.TERRITORY,
    h.COUNTRY,
    h.ACCOUNT_NUMBER,
    h.SHIP_METHOD,
    h.SUB_TOTAL,
    h.TAX_AMOUNT,
    h.FREIGHT,
    h.TOTAL_DUE,
    d.TOTAL_LINE_AMOUNT,
    d.TOTAL_QUANTITY,
    d.TOTAL_PRICE,
    d.TOTAL_DISCOUNT
FROM order_header h
LEFT JOIN order_details_aggregated d
  ON h.SALES_ORDER_ID = d.SALES_ORDER_ID
LEFT JOIN {{ ref('dim_customers') }} c
    ON h.CUSTOMER_ID = c.CUSTOMER_ID
    and h.ACCOUNT_NUMBER = c.ACCOUNT_NUMBER
    AND h.TERRITORY = c.TERRITORY
LEFT JOIN {{ ref('dim_persons') }} p
    ON c.PERSON_ID = p.PERSON_ID
LEFT JOIN {{ ref('dim_stores') }} s
    ON c.STORE_ID = s.STORE_ID
LEFT JOIN {{ ref('dim_products') }} pr
    ON d.PRODUCT_ID = pr.PRODUCT_ID 