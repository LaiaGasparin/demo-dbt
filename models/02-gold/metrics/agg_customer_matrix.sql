{{ config(materialized='table') }}

SELECT 
    customer_key,
    customer_name,
    customer_tier,
    region_name,
    MIN(order_date) as first_order,
    MAX(order_date) as last_order,
    COUNT(DISTINCT order_key) as total_orders,
    SUM(net_sales) as lifetime_value,
    AVG(net_sales) as avg_order_value
FROM {{ ref('fact_sales') }}
GROUP BY 1,2,3,4