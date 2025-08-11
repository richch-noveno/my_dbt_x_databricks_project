
    SELECT 
        SALES_ORDER_ID,
        SUM(ORDER_QUANTITY) AS total_quantity,
        SUM(LINE_TOTAL) AS total_line_amount,
        SUM(UNIT_PRICE_DISCOUNT) AS total_discount
    FROM {{ ref('silver_sales_order_details') }}
    GROUP BY SALES_ORDER_ID

