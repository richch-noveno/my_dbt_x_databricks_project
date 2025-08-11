/*
 This model processes the sales orders details data from the bronze layer
 and stores it in the silver layer with incremental updates.
 It includes transformations for date fields and applies timezone conversion to West Australia (Perth).
 */
{{ incremental_config() }}

SELECT
  salesOrderId AS SALES_ORDER_ID,
  salesOrderDetailId AS SALES_ORDER_DETAIL_ID,
  carrierTrackingNumber AS CARRIER_TRACKING_NUMBER,
  orderQuantity AS ORDER_QUANTITY,
  productId AS PRODUCT_ID,
  productName AS PRODUCT_NAME,
  specialOfferId AS SPECIAL_OFFER_ID,
  unitPrice AS UNIT_PRICE,
  unitPriceDiscount AS UNIT_PRICE_DISCOUNT,
  lineTotal AS LINE_TOTAL,
  {{ from_utc_to_WA() }} AS MODIFIED_DATE,
  {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE
FROM {{ source('bronze_adworks_rich', 'salesorderdetails') }}
WHERE salesOrderDetailId IS NOT NULL
  AND salesOrderId IS NOT NULL

{% if is_incremental() %}
    AND {{ from_utc_to_WA() }} > ( SELECT MAX(MODIFIED_DATE) FROM {{ this }} )
{% endif %}