{{ config(
    materialized='table',
    tags=['metrics', 'gold'],
    on_schema_change='sync_all_columns',
    cluster_by=['date_month', 'discount_band']
) }}

WITH base AS (
    SELECT
        date_month,
        CASE 
            WHEN discount = 0 THEN '0%'
            WHEN discount <= 0.1 THEN '0-10%'
            WHEN discount <= 0.2 THEN '10-20%'
            WHEN discount <= 0.3 THEN '20-30%'
            WHEN discount <= 0.4 THEN '30-40%'
            WHEN discount <= 0.5 THEN '40-50%'
            ELSE '50%+'
        END AS discount_band,
        quantity,
        extended_net,
        extended_cost
    FROM {{ ref('fact_sales') }}  -- Use denormalized fact table
)

SELECT
    date_month,
    discount_band,
    SUM(quantity) AS units,
    SUM(extended_net) AS revenue_net,
    SUM(extended_cost) AS cost,
    SUM(extended_net) - SUM(extended_cost) AS gross_margin,
    ROUND(
        (SUM(extended_net) - SUM(extended_cost)) / NULLIF(SUM(extended_net), 0),
        4
    ) AS margin_pct
FROM base
GROUP BY 
    date_month,
    discount_band
ORDER BY 
    date_month,
    discount_band