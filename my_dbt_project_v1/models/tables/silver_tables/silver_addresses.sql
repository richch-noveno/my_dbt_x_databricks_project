SELECT 
    addressId as ADDRESS_ID,
    CONCAT_WS(' ', addressLine1, addressLine2) AS STREET,
    addressLine1  AS ADDRESS_LINE_1,   
    addressLine2  AS ADDRESS_LINE_2,
    city  AS CITY,
    state  AS STATE,
    country  AS COUNTRY,
    postalCode  AS POSTAL_CODE,
    {{ from_utc_to_WA() }} AS MODIFIED_DATE,
    {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE
FROM {{ source('bronze_adworks_rich', 'addresses') }}
WHERE addressId IS NOT NULL 