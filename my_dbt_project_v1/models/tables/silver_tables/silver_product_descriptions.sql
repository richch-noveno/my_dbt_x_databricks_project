SELECT
    productDescriptionId AS PRODUCT_DESCRIPTION_ID,
    description AS PRODUCT_DESCRIPTION,
    cultureCode AS CULTURE_CODE,
    {{ from_utc_to_WA() }} AS MODIFIED_DATE,
    {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE
FROM {{ source('bronze_adworks_rich', 'productdescriptions') }}
WHERE productDescriptionId IS NOT NULL