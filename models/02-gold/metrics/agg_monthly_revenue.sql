{{ config(
    materialized='table',
    cluster_by=['order_year', 'order_month']
) }}

SELECT 
    order_year,
    order_month,
    order_month_name,
    region_name,
    customer_tier,
    COUNT(DISTINCT order_key) as order_count,
    COUNT(DISTINCT customer_key) as unique_customers,
    SUM(quantity) as total_units,
    SUM(net_sales) as total_revenue,
    AVG(net_sales) as avg_order_value,
    SUM(discount_amount) as total_discounts
FROM {{ ref('fact_sales') }}
GROUP BY 1,2,3,4,5