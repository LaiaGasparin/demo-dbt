{{ config(materialized='table') }}

SELECT 
    DATE(order_date) as date_key,
    COUNT(DISTINCT order_key) as total_orders,
    COUNT(DISTINCT customer_key) as unique_customers,
    SUM(quantity) as units_sold,
    SUM(net_sales) as revenue
FROM {{ ref('fact_sales') }}
GROUP BY 1