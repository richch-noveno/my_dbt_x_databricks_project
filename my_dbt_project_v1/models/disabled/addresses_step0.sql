SELECT 
    CAST(addressId AS INT) AS ADDRESS_ID,
    CAST(addressLine1 AS STRING) AS ADDRESS_LINE_1,   
    CAST(addressLine2 AS STRING) AS ADDRESS_LINE_2,
    CAST(city AS STRING) AS CITY,
    CAST(state AS STRING) AS STATE,
    CAST(country AS STRING) AS COUNTRY,
    CAST(postalCode AS STRING) AS POSTAL_CODE,
    CAST(modifiedDate AS DATE) AS MODIFIED_DATE,
    UpdatedDate AS UPDATED_DATE
FROM {{ source('bronze_adworks_rich', 'addresses') }}