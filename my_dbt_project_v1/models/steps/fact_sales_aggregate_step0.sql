SELECT 
        SALES_ORDER_ID,
        ORDER_DATE,
        CUSTOMER_ID,
        SALES_PERSON_ID,
        TERRITORY,
        SHIP_METHOD,
        SUB_TOTAL,
        TAX_AMOUNT,
        FREIGHT,
        TOTAL_DUE
FROM {{ ref('silver_sales_orders') }}
