{{ config(
    materialized='incremental',
    unique_key='order_date',
    on_schema_change='merge'
) }}

SELECT 
    order_date,
    COUNT(DISTINCT order_key) as orders,
    COUNT(DISTINCT customer_key) as customers,
    COUNT(DISTINCT part_key) as products_sold,
    SUM(net_sales) as revenue,
    AVG(net_sales) as avg_basket,
    MAX(net_sales) as largest_order
FROM {{ ref('fact_sales') }}

{% if is_incremental() %}
  WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}

GROUP BY 1