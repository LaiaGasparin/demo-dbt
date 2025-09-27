-- models/gold/agg_promo_impact.sql
{{ config(materialized='table', tags=['metrics']) }}

WITH base AS (
    SELECT
        YEAR(f.order_date) as year,
        MONTH(f.order_date) as month_num,
        (YEAR(f.order_date) * 100 + MONTH(f.order_date)) as month_key,
        f.net_sales,
        CASE 
            WHEN p.part_type ILIKE 'PROMO%' THEN f.net_sales 
            ELSE 0 
        END as promo_net_sales
    FROM {{ ref('fact_sales') }} f
    JOIN {{ ref('dim_product') }} p ON f.part_key = p.part_key
),

monthly_metrics AS (
    SELECT
        year,
        month_num,
        month_key,
        SUM(promo_net_sales) as promo_revenue,
        SUM(net_sales) as total_revenue,
        CASE 
            WHEN SUM(net_sales) = 0 THEN 0
            ELSE ROUND(100.0 * SUM(promo_net_sales) / SUM(net_sales), 2)
        END as promo_revenue_pct
    FROM base
    GROUP BY 1, 2, 3
)

SELECT
    year,
    month_num,
    month_key,
    promo_revenue,
    total_revenue,
    
    -- Revenue in billions
    ROUND(total_revenue / 1000000000, 3) as revenue_billions,
    
    -- Promo percentage
    promo_revenue_pct,
    
    -- Month-over-month calculations with NULL for first month
    CASE 
        WHEN LAG(total_revenue) OVER (ORDER BY month_key) IS NULL THEN NULL
        ELSE ROUND(
            100.0 * (total_revenue - LAG(total_revenue) OVER (ORDER BY month_key)) / 
            LAG(total_revenue) OVER (ORDER BY month_key), 
            2
        )
    END as revenue_growth_pct,
    
    -- Promo percentage point change (not delta from 0)
    CASE
        WHEN LAG(promo_revenue_pct) OVER (ORDER BY month_key) IS NULL THEN NULL
        ELSE ROUND(
            promo_revenue_pct - LAG(promo_revenue_pct) OVER (ORDER BY month_key), 
            2
        )
    END as promo_pct_delta

FROM monthly_metrics
ORDER BY year, month_num