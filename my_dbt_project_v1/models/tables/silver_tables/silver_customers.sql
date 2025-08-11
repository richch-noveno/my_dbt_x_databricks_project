SELECT
  customerId AS CUSTOMER_ID,
  personId AS PERSON_ID,
  storeId AS STORE_ID,
  territory AS TERRITORY,
  accountNumber AS ACCOUNT_NUMBER,
  {{ from_utc_to_WA() }} AS MODIFIED_DATE,
  {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE 
FROM {{ source('bronze_adworks_rich', 'customers') }}
WHERE customerId IS NOT NULL