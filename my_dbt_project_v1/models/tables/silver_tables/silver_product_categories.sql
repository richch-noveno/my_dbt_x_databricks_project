SELECT
  productCategoryId AS PRODUCT_CATEGORY_ID,
  name AS NAME,
  {{ from_utc_to_WA() }} AS MODIFIED_DATE,
  {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE
FROM {{ source('bronze_adworks_rich', 'productcategories') }}
WHERE productCategoryId IS NOT NULL