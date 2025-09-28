{{ config(
    materialized='table',
    tags=['metrics', 'gold'],
    on_schema_change='sync_all_columns',
    cluster_by=['date_month']
) }}

WITH base AS (
    SELECT
        date_month,
        is_promo,
        extended_net
    FROM {{ ref('fact_sales') }}  -- Use denormalized fact table
),

monthly_promo AS (
    SELECT
        date_month,
        SUM(CASE WHEN is_promo = true THEN extended_net ELSE 0 END) AS promo_revenue,
        SUM(extended_net) AS total_revenue
    FROM base
    GROUP BY date_month
),

final AS (
    SELECT
        date_month,
        promo_revenue,
        total_revenue,
        ROUND(
            100.0 * promo_revenue / NULLIF(total_revenue, 0),
            2
        ) AS promo_revenue_pct,
        ROUND(
            100.0 * (total_revenue - LAG(total_revenue) OVER (ORDER BY date_month)) 
            / NULLIF(LAG(total_revenue) OVER (ORDER BY date_month), 0),
            2
        ) AS revenue_growth_pct,
        ROUND(
            (100.0 * promo_revenue / NULLIF(total_revenue, 0)) - 
            LAG(100.0 * promo_revenue / NULLIF(total_revenue, 0)) OVER (ORDER BY date_month),
            2
        ) AS promo_share_delta_pp
    FROM monthly_promo
)

SELECT * FROM final
ORDER BY date_month