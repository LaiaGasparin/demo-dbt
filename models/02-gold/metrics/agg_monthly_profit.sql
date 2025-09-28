{{ config(
    materialized='table',
    tags=['metrics', 'gold'],
    on_schema_change='sync_all_columns',
    cluster_by=['date_month']
) }}

WITH base AS (
    SELECT
        date_month,
        quantity,
        extended_net,
        extended_cost
    FROM {{ ref('fact_sales') }}  -- Use the denormalized fact table
),

monthly_metrics AS (
    SELECT
        date_month,
        SUM(quantity) AS units,
        SUM(extended_net) AS revenue_net,
        SUM(extended_cost) AS cost,
        SUM(extended_net) - SUM(extended_cost) AS gross_margin,
        ROUND(
            (SUM(extended_net) - SUM(extended_cost)) / NULLIF(SUM(extended_net), 0),
            4
        ) AS margin_pct
    FROM base
    GROUP BY date_month
),

final AS (
    SELECT
        date_month,
        units,
        revenue_net,
        cost,
        gross_margin,
        margin_pct,
        ROUND(
            100.0 * (revenue_net - LAG(revenue_net) OVER (ORDER BY date_month)) 
            / NULLIF(LAG(revenue_net) OVER (ORDER BY date_month), 0),
            2
        ) AS revenue_growth_pct
    FROM monthly_metrics
)

SELECT * FROM final
ORDER BY date_month