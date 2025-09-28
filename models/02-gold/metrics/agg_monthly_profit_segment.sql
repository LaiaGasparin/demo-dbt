{{ config(
    materialized='table',
    tags=['metrics', 'gold'],
    on_schema_change='sync_all_columns',
    cluster_by=['date_month', 'reporting_region', 'market_segment']
) }}

WITH base AS (
    SELECT
        date_month,
        reporting_region,
        market_segment,
        customer_tier,
        quantity,
        extended_net,
        extended_cost
    FROM {{ ref('fact_sales') }}  -- Use the denormalized fact table
)

SELECT
    date_month,
    reporting_region,
    market_segment,
    customer_tier,
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
    reporting_region,
    market_segment,
    customer_tier
ORDER BY 
    date_month,
    reporting_region,
    market_segment,
    customer_tier