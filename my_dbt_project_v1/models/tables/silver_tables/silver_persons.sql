SELECT
  personId AS PERSON_ID,
  personType AS PERSON_TYPE,
  nameStyle AS NAME_STYLE,
  title AS TITLE,
  --Note: add a code that will separete middle initial and midddle name
    /** Some of the value in middle_name has only 1 character, treat it as middle initial
    **/
  CONCAT_WS(' ',
            title,
            firstName, 
            CASE WHEN LENGTH(middleName) = 1 THEN CONCAT(middleName, '.') 
            else middleName END,  
            lastName,
            suffix
            ) AS FULL_NAME,
  firstName AS FIRST_NAME,
  middleName AS MIDDLE_NAME,
  lastName AS LAST_NAME,
  suffix AS SUFFIX,
  emailPromotion AS EMAIL_PROMOTION,
  additionalContactInfo AS ADDITIONAL_CONTACT_INFO,
  CAST(TotalPurchaseYTD AS DECIMAL(10,2)) AS TOTAL_PURCHASE_YTD,
  {{ from_utc_to_WA() }} AS MODIFIED_DATE,
  {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE 
FROM {{ source('bronze_adworks_rich', 'persons') }}
WHERE personId IS NOT NULL