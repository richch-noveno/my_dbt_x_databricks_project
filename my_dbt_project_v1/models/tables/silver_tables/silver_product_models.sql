SELECT
    productModelId AS PRODUCT_MODEL_ID,
    name AS NAME,
    catalogDescription AS CATALOG_DESCRIPTION,
    instructions AS INSTRUCTIONS,
    {{ from_utc_to_WA() }} AS MODIFIED_DATE,
    {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE
FROM {{ source('bronze_adworks_rich', 'productmodels') }}
WHERE productModelId IS NOT NULL