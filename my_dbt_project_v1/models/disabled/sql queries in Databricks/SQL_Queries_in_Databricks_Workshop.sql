--SQL Queries in Databricks Workshop

----New Query 2025-07-22 3:39pm


-- tables = [
--     "addresses", "customers", "products", "productCategories", "productDescriptions",
--     "productModels", "salesOrderDetails", "salesOrders", "stores", "persons"
-- ]

-- ignore other queries then generate a new code that call and elaborate all column names of table adventureworks_dev.bronze_adworks_rich.persons then limit to 5

DESCRIBE adventureworks_dev.bronze_adworks_rich.customers;


SELECT
  customerId AS customer_id,
  personId AS person_id,
  storeId AS store_id,
  territory AS territory,
  accountNumber AS account_number,
  modifiedDate AS modified_date
FROM adventureworks_dev.bronze_adworks_rich.customers
LIMIT 5;


SELECT
  personId AS person_id,
  personType AS person_type,
  nameStyle AS name_style,
  title AS title,
  firstName AS first_name,
  middleName AS middle_name,
  lastName AS last_name,
  suffix AS suffix,
  emailPromotion AS email_promotion,
  additionalContactInfo AS additional_contact_info,
  TotalPurchaseYTD AS total_purchase_ytd,
  modifiedDate AS modified_date
FROM adventureworks_dev.bronze_adworks_rich.persons;
-- LIMIT 5;

SELECT
  productCategoryId AS PRODUCT_CATEGORY_ID,
  name AS NAME,
  modifiedDate AS MODIFIED_DATE
FROM adventureworks_dev.bronze_adworks_rich.productCategories
LIMIT 5;

DESCRIBE adventureworks_dev.bronze_adworks_rich.products;

SELECT * FROM adventureworks_dev.bronze_adworks_rich.products;


SELECT
  date_format(CAST(modifiedDate AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS formatted_modified_date,
  TO_TIMESTAMP(cast(modifiedDate AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS formatted_modified_date2,
  CAST(modifiedDate AS TIMESTAMP) AS MODIFIED_DATE,
  current_timestamp()   AS UPDATE_DATE
FROM adventureworks_dev.bronze_adworks_rich.pRODUCTS
LIMIT 5;

SELECT
  personId AS person_id,
  TRY_TO_TIMESTAMP(modifiedDate, 'yyyy-MM-dd HH:mm:ss') AS formatted_modified_date
FROM adventureworks_dev.bronze_adworks_rich.persons
LIMIT 5;


/*
PLEASE GENERATE A NEW CODE OF SELECTING ALL COLUMN NAMES OF adventureworks_dev.bronze_adworks_rich.salesorderdetails USING ALIAS  AND APPLY UPPER CASE TO ALL COLUMN NAMES AND THEN PUT UNDERSCORE IN EVERY WORD FORMED
*/
DESCRIBE adventureworks_dev.bronze_adworks_rich.salesorderdetails;

SELECT * FROM adventureworks_dev.bronze_adworks_rich.salesorderdetails
where salesorderid is not null
and salesorderdetailid is not null;

SELECT
  SalesOrderID AS SALES_ORDER_ID,
  SalesOrderDetailID AS SALES_ORDER_DETAIL_ID,
  CarrierTrackingNumber AS CARRIER_TRACKING_NUMBER,
  OrderQty AS ORDER_QTY,
  ProductID AS PRODUCT_ID,
  SpecialOfferID AS SPECIAL_OFFER_ID,
  UnitPrice AS UNIT_PRICE,
  UnitPriceDiscount AS UNIT_PRICE_DISCOUNT,
  LineTotal AS LINE_TOTAL,
  ModifiedDate AS MODIFIED_DATE
FROM adventureworks_dev.bronze_adworks_rich.salesorderdetails
LIMIT 5;

DESCRIBE adventureworks_dev.bronze_adworks_rich.salesorders;

SELECT * FROM adventureworks_dev.bronze_adworks_rich.salesorders
where salesorderid is not null;

SELECT
  SalesOrderID AS SALES_ORDER_ID,
  RevisionNumber AS REVISION_NUMBER,
  OrderDate AS ORDER_DATE,
  DueDate AS DUE_DATE,
  ShipDate AS SHIP_DATE,
  Status AS STATUS,
  OnlineOrderFlag AS ONLINE_ORDER_FLAG,
  SalesOrderNumber AS SALES_ORDER_NUMBER,
  PurchaseOrderNumber AS PURCHASE_ORDER_NUMBER,
  AccountNumber AS ACCOUNT_NUMBER,
  CustomerID AS CUSTOMER_ID,
  SalesPersonID AS SALES_PERSON_ID,
  TerritoryID AS TERRITORY_ID,
  BillToAddressID AS BILL_TO_ADDRESS_ID,
  ShipToAddressID AS SHIP_TO_ADDRESS_ID,
  ShipMethodID AS SHIP_METHOD_ID,
  CreditCardID AS CREDIT_CARD_ID,
  CreditCardApprovalCode AS CREDIT_CARD_APPROVAL_CODE,
  CurrencyRateID AS CURRENCY_RATE_ID,
  SubTotal AS SUB_TOTAL,
  TaxAmt AS TAX_AMT,
  Freight AS FREIGHT,
  TotalDue AS TOTAL_DUE,
  Comment AS COMMENT,
  ModifiedDate AS MODIFIED_DATE
FROM adventureworks_dev.bronze_adworks_rich.salesorders
LIMIT 5;

DESCRIBE adventureworks_dev.bronze_adworks_rich.sTORES;

SELECT * FROM adventureworks_dev.bronze_adworks_rich.stores;

--to check if all values are numerics:
SELECT * FROM adventureworks_dev.bronze_adworks_rich.stores
WHERE NOT SquareFeet RLIKE '^[0-9]+$';


-----------------------------------------------------------------------------------------
--New Query 2025-07-23 9:20am-----------------------------------

SELECT
  personId AS PERSON_ID,
  personType AS PERSON_TYPE,
  nameStyle AS NAME_STYLE,
  title AS TITLE,
  --Note: add a code that will separete middle initial and midddle name
    /** Some of the value in middle_name has only 1 character
    **/
  CONCAT_WS(' ',
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
  CAST(modifiedDate AS TIMESTAMP) AS MODIFIED_DATE,
  current_timestamp() AS UPDATE_DATE 
FROM adventureworks_dev.bronze_adworks_rich.persons
WHERE personId IS NOT NULL;

---------------------------------------------------------------
----Sample queries for building fact aggregates

select distinct CUSTOMER_ID from adventureworks_dev.silver_adworks_rich.silver_sales_orders ;

select * from adventureworks_dev.silver_adworks_rich.silver_customers 
where person_id is not null; --all nulls

select * from adventureworks_dev.silver_adworks_rich.silver_persons;

select * from adventureworks_dev.silver_adworks_rich.silver_persons;

select * from adventureworks_dev.information_schema.columns
WHERE table_SCHEMA  LIKE '%_rich'
and COLUMN_NAME in ( 'PERSON_ID', 'CUSTOMER_ID', 'STORE_ID', 'PRODUCT_ID');
-- WHERE COLUMN_NAME LIKE '%TERRITORY%';
--'ACCOUNT_NUMBER', 'TERRITORY',



WITH order_header AS (
    SELECT 
        SALES_ORDER_ID,
        ORDER_DATE,
        CUSTOMER_ID,
        SALES_PERSON_ID,
        TERRITORY,
        ACCOUNT_NUMBER,
        SHIP_METHOD,
        SUB_TOTAL,
        TAX_AMOUNT,
        FREIGHT,
        TOTAL_DUE
    FROM silver_sales_orders
),

order_details_aggregated AS (
    SELECT 
        SALES_ORDER_ID,
        PRODUCT_ID,
        SUM(ORDER_QUANTITY) AS total_quantity,
        SUM(LINE_TOTAL) AS total_line_amount,
        SUM(UNIT_PRICE_DISCOUNT) AS total_discount
    FROM silver_sales_order_details
    GROUP BY SALES_ORDER_ID
    , PRODUCT_ID
)


SELECT 
    h.SALES_ORDER_ID,
    d.PRODUCT_ID,
    h.ORDER_DATE,
    h.CUSTOMER_ID,
    h.SALES_PERSON_ID,
    s.STORE_ID,
    h.TERRITORY,
    h.ACCOUNT_NUMBER,
    h.SHIP_METHOD,
    h.SUB_TOTAL,
    h.TAX_AMOUNT,
    h.FREIGHT,
    h.TOTAL_DUE,
    d.total_quantity,
    d.total_line_amount,
    d.total_discount
FROM order_header h
LEFT JOIN order_details_aggregated d
  ON h.SALES_ORDER_ID = d.SALES_ORDER_ID
LEFT JOIN silver_customers c
    ON h.CUSTOMER_ID = c.CUSTOMER_ID
    and h.ACCOUNT_NUMBER = c.ACCOUNT_NUMBER
    AND h.TERRITORY = c.TERRITORY
LEFT JOIN silver_persons p
    ON c.PERSON_ID = p.PERSON_ID
LEFT JOIN silver_stores s
    ON c.STORE_ID = s.STORE_ID
LEFT JOIN adventureworks_dev.gold_adworks_rich.dim_products pr
    ON d.PRODUCT_ID = pr.PRODUCT_ID 

;

--43668	760	2011-05-31	29614	282	Canada (Canada)	10-4020-000514	CARGO TRANSPORT 5	35944.1562	3461.7654	1081.8017	40487.7233	7	2936.2123	0
--43668	2011-05-31	29614	282	Canada (Canada)	10-4020-000514	CARGO TRANSPORT 5	35944.1562	3461.7654	1081.8017	40487.7233	62	24053.860999999997	0
SELECT C.CUSTOMER_ID,
P.PERSON_ID,
S.STORE_ID
FROM adventureworks_dev.silver_adworks_rich.silver_customers C
LEFT JOIN adventureworks_dev.silver_adworks_rich.silver_PERSONS P
    ON C.PERSON_ID = P.PERSON_ID
LEFT JOIN adventureworks_dev.silver_adworks_rich.silver_STORES S
    ON C.STORE_ID = S.STORE_ID;

SELECT SOD.PRODUCT_ID, pr.PRODUCT_ID
FROM adventureworks_dev.silver_adworks_rich.silver_sales_order_details SOD
left JOIN adventureworks_dev.gold_adworks_rich.dim_products pr
    ON SOD.PRODUCT_ID = pr.PRODUCT_ID ;

SELECT C.CUSTOMER_ID,
SP.CUSTOMER_ID
FROM adventureworks_dev.silver_adworks_rich.silver_customers C
LEFT JOIN adventureworks_dev.silver_adworks_rich.silver_sales_orders SP
    ON C.CUSTOMER_ID = SP.CUSTOMER_ID;

SELECT C.PERSON_ID,
SP.SALES_PERSON_ID
FROM adventureworks_dev.silver_adworks_rich.silver_persons C
LEFT JOIN adventureworks_dev.silver_adworks_rich.silver_sales_orders SP
    ON C.PERSON_ID = SP.SALES_PERSON_ID;


-----------------------------------------------------------------------------------------------
--Testing queries pattern to dbt models


-- SELECT 
-- customerID,
-- personID,
-- storeID,
-- territory,
-- accountnumber,
-- modifiedDate
-- FROM customers

-- accountNumber,
-- customerld,
-- modifiedDate,
-- personld,
-- storeld,
-- territory

SELECT COUNT(*) FROM adventureworks_dev.bronze_adworks_rich.salesorders
where salesOrderId is not null;

SELECT modifiedDate,
from_utc_timestamp(modifiedDate, 'Australia/Perth') AS modifiedDate_AWST,
current_timestamp() as currenttime,
from_utc_timestamp(current_timestamp(), 'Australia/Perth') AS currentDate_AWST
FROM adventureworks_dev.bronze_adworks_rich.addresses f
where modifiedDate = '2013-12-21T10:09:29.423';


SELECT * FROM adventureworks_dev.silver_adworks_rich_incremental.silver_sales_orders;

WITH max_modified AS (
            SELECT MAX( from_utc_timestamp(modified_Date, 'Australia/Perth') ) AS MAX_MOD_DATE
            FROM adventureworks_dev.silver_adworks_rich.silver_sales_orders
        )
        SELECT *
        FROM adventureworks_dev.bronze_adworks_rich.salesorders
        WHERE  from_utc_timestamp(modifiedDate, 'Australia/Perth')  > (
            SELECT MAX_MOD_DATE FROM max_modified
        ) ;

-- select COALESCE(MAX(from_utc_timestamp('MODIFIED_DATE', 'Australia/Perth')), '1900-01-01') as max_mod_date from adventureworks_dev.silver_adworks_rich.silver_sales_orders;

select
    SALES_ORDER_ID as unique_field,
    count(*) as n_records

from `adventureworks_dev`.`silver_adworks_rich`.`silver_sales_order_details`
where SALES_ORDER_ID is not null
group by SALES_ORDER_ID
having count(*) > 1;

select
*
from `adventureworks_dev`.`silver_adworks_rich`.`silver_sales_order_details`
where SALES_ORDER_ID = '43668' ;

-------------------------------------------------------------------------------------------
--To Drop specific column query



-- ALTER TABLE adventureworks_dev.bronze_adworks_rich.addresses SET TBLPROPERTIES (
--   'delta.columnMapping.mode' = 'name'
-- );

ALTER TABLE adventureworks_dev.bronze_adworks_rich.addresses SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.customers SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.persons SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.productcategories SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.productdescriptions SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.productmodels SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.products SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.salesorderdetails SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.salesorders SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);
ALTER TABLE adventureworks_dev.bronze_adworks_rich.stores SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);

-- ALTER TABLE adventureworks_dev.bronze_adworks_rich.addresses DROP COLUMN  UpdatedDate;
-- CT
ALTER TABLE adventureworks_dev.bronze_adworks_rich.productcategories DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.salesorderdetails DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.salesorders DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.productdescriptions DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.productmodels DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.persons DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.stores DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.products DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.addresses DROP COLUMN UpdatedDate;
ALTER TABLE adventureworks_dev.bronze_adworks_rich.customers DROP COLUMN UpdatedDate;

--to change the datatype of 1 column:
-- ALTER TABLE adventureworks_dev.silver_adworks_rich.silver_persons ALTER COLUMN TOTAL_PURCHASE_YTD TYPE DECIMAL(10, 2);

SELECT CONCAT('ALTER TABLE ', table_catalog, '.', table_schema, '.', table_name, ' DROP COLUMN UpdatedDate',';') AS CT
FROM adventureworks_dev.information_schema.tables
WHERE table_schema = 'bronze_adworks_rich';



show tblproperties adventureworks_dev.bronze_adworks_rich.addresses;
show tblproperties adventureworks_dev.bronze_adworks_rich.customers;


SELECT CONCAT('DROP TABLE IF EXISTS ', table_catalog, '.', table_schema, '.', table_name, ';') AS CT
FROM adventureworks_dev.information_schema.tables
WHERE table_schema = 'bronze_adworks_rich';



DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich.silver_sales_orders;
DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich_incremental.silver_sales_orders;
DROP SCHEMA IF EXISTS adventureworks_dev.silver_adworks_rich_silver_adworks_rich;
DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich_silver_adworks_rich.silver_sales_orders;

DROP SCHEMA IF EXISTS adventureworks_dev.silver_adworks_rich_incremental;


DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.productcategories;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.salesorderdetails;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.salesorders;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.productdescriptions;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.productmodels;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.persons;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.stores;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.products;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.addresses;
DROP TABLE IF EXISTS adventureworks_dev.bronze_adworks_rich.customers;

--to drop the gold tables:

DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich.dim_products;
DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich.dim_persons;
DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich.dim_stores;
DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich.fact_sales_orders;
DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich.dim_addresses;
DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich.dim_customers;


SELECT CONCAT('DROP TABLE IF EXISTS ', table_catalog, '.', table_schema, '.', table_name,';') AS DT
FROM adventureworks_dev.information_schema.tables
WHERE table_schema = '_silver_adworks_rich';


-- DROP TABLE IF EXISTS adventureworks_dev._gold_adworks_rich.dim_stores;
-- DROP TABLE IF EXISTS adventureworks_dev._gold_adworks_rich.dim_products;
-- DROP TABLE IF EXISTS adventureworks_dev._gold_adworks_rich.fact_sales_orders;
-- DROP TABLE IF EXISTS adventureworks_dev._gold_adworks_rich.dim_persons;
-- DROP TABLE IF EXISTS adventureworks_dev._gold_adworks_rich.dim_customers;
-- DROP TABLE IF EXISTS adventureworks_dev._gold_adworks_rich.dim_addresses;
-- DROP TABLE IF EXISTS adventureworks_dev.silver_adworks_rich_gold_adworks_rich.fact_sales_orders;


DROP TABLE IF EXISTS adventureworks_dev.default_silver_adworks_rich.silver_product_models;
DROP TABLE IF EXISTS adventureworks_dev.default_silver_adworks_rich.silver_product_descriptions;
DROP TABLE IF EXISTS adventureworks_dev.default_silver_adworks_rich.silver_products;
DROP TABLE IF EXISTS adventureworks_dev.default_silver_adworks_rich.silver_persons;
DROP TABLE IF EXISTS adventureworks_dev.default_silver_adworks_rich.silver_addresses;
DROP TABLE IF EXISTS adventureworks_dev.default_silver_adworks_rich.silver_customers;
DROP TABLE IF EXISTS adventureworks_dev.default_silver_adworks_rich.silver_stores;
DROP TABLE IF EXISTS adventureworks_dev.default_silver_adworks_rich.silver_product_categories;


DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_sales_order_details;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_product_models;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_sales_orders;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_addresses;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_stores;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_persons;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_product_descriptions;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_product_categories;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_customers;
DROP TABLE IF EXISTS adventureworks_dev._silver_adworks_rich.silver_products;

DROP SCHEMA IF EXISTS adventureworks_dev._gold_adworks_rich;
DROP SCHEMA IF EXISTS adventureworks_dev.silver_adworks_rich_gold_adworks_rich;

DROP SCHEMA IF EXISTS adventureworks_dev.default_silver_adworks_rich;
DROP SCHEMA IF EXISTS adventureworks_dev._silver_adworks_rich;

