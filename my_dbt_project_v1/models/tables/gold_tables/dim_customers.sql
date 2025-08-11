--dim_customers
SELECT  
  CUSTOMER_ID,
  C.PERSON_ID,
  FULL_NAME,
  TERRITORY,
  ACCOUNT_NUMBER,
  STORE_ID,
  regexp_extract(C.TERRITORY, '\\((.*?)\\)', 1) AS COUNTRY
FROM {{ ref('silver_customers') }} C
LEFT JOIN {{ ref('silver_persons') }} P
  ON C.PERSON_ID = P.PERSON_ID
WHERE C.CUSTOMER_ID IS NOT NULL