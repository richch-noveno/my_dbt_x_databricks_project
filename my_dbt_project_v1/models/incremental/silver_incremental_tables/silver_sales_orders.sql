/*
 This model processes the sales orders data from the bronze layer
 and stores it in the silver layer with incremental updates.
 It includes transformations for date fields and applies timezone conversion to West Australia (Perth).
 */


{{ incremental_config() }}

SELECT
  SalesOrderID AS SALES_ORDER_ID,
  RevisionNumber AS REVISION_NUMBER,
  date({{ from_utc_to_WA('OrderDate') }}) AS ORDER_DATE,
  date({{ from_utc_to_WA('DueDate') }}) AS DUE_DATE,
  date({{ from_utc_to_WA('ShipDate') }}) AS SHIP_DATE,
  Status AS STATUS,
  isOrderedOnline AS IS_ORDERED_ONLINE_FLAG,
  SalesOrderNumber AS SALES_ORDER_NUMBER,
  PurchaseOrderNumber AS PURCHASE_ORDER_NUMBER,
  AccountNumber AS ACCOUNT_NUMBER,
  CustomerID AS CUSTOMER_ID,
  SalesPersonID AS SALES_PERSON_ID,
  Territory AS TERRITORY,
  BillToAddress AS BILL_TO_ADDRESS,
  ShipToAddress AS SHIP_TO_ADDRESS,
  ShipMethod AS SHIP_METHOD,
  CreditCardID AS CREDIT_CARD_ID,
  CreditCardApprovalCode AS CREDIT_CARD_APPROVAL_CODE,
  CurrencyRateID AS CURRENCY_RATE_ID,
  SubTotal AS SUB_TOTAL,
  taxAmount AS TAX_AMOUNT,
  freight AS FREIGHT,
  totalDue AS TOTAL_DUE,
  comment AS COMMENT,
  -- CAST(modifiedDate AS TIMESTAMP) AS MODIFIED_DATE,
  {{ from_utc_to_WA() }} AS MODIFIED_DATE,
  {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE
FROM {{ source('bronze_adworks_rich', 'salesorders') }}
WHERE SalesOrderID IS NOT NULL

{% if is_incremental() %}
    AND {{ from_utc_to_WA() }} > ( SELECT MAX(MODIFIED_DATE) FROM {{ this }} )
{% endif %}

