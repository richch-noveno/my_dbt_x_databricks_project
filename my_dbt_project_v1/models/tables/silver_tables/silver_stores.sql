SELECT
    storeId AS STORE_ID,
    name as NAME,
    salesPersonId AS SALES_PERSON_ID,
    CAST(AnnualSales AS DOUBLE) AS ANNUAL_SALES,
    CAST(AnnualRevenue AS DOUBLE) AS ANNUAL_REVENUE,
    BankName AS BANK_NAME,
    BusinessType AS BUSINESS_TYPE,
    CAST(YearOpened AS INTEGER) AS YEAR_OPENED,
    Specialty AS SPECIALTY,
    CAST(SquareFeet AS INTEGER) AS SQUARE_FEET,
    Brands AS BRANDS,
    Internet AS INTERNET,
    CAST(NumberEmployees AS INTEGER) AS NUMBER_OF_EMPLOYEES,
    {{ from_utc_to_WA() }} AS MODIFIED_DATE,
    {{ from_utc_to_WA(current_timestamp()) }} AS UPDATE_DATE
FROM {{ source('bronze_adworks_rich', 'stores') }}
WHERE storeId IS NOT NULL