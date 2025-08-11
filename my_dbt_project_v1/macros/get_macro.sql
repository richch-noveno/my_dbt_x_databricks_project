-- This macro generates a schema name based on a custom schema name or defaults to the target schema
{% macro generate_schema_name(custom_schema_name, node) %}
    {% if custom_schema_name is not none %}
        {{ custom_schema_name }}
    {% else %}
        {{ target.schema }}
    {% endif %}
{% endmacro %}


--This macro sets the materialization to incremental, defines a unique key
--{{ incremental_config() }}

{% macro incremental_config() %}
  {% set unique_keys = {
    "silver_sales_orders": "SALES_ORDER_ID",
    "silver_sales_order_details": ["SALES_ORDER_ID","SALES_ORDER_DETAIL_ID"],
  } %}

  {% set model_name = model.name %}
  {% set key = unique_keys.get(model_name, "id") %}  {# fallback key #}

  {{ config(
    materialized='incremental',
    unique_key=key,
    incremental_strategy='merge',
    file_format='delta'
  ) }}
{% endmacro %}


-- This macro converts a UTC timestamp to a specific timezone, defaulting to Australia/Perth
--{{ from_utc_to_WA()}}
{% macro from_utc_to_WA(expression='modifiedDate', timezone='Australia/Perth') %}
  {% set is_string = expression is string %}
  {% if is_string %}
    from_utc_timestamp({{ expression }}, '{{ timezone }}')
  {% else %}
    from_utc_timestamp({{ expression }}, '{{ timezone }}')
  {% endif %}
{% endmacro %}

