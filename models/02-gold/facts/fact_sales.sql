-- models/gold/fact_sales.sql
{{ config(
    materialized='table', 
    on_schema_change='sync_all_columns',
    cluster_by=['order_date', 'region_name']
) }}

WITH fact_enriched AS (
    SELECT
        -- Keys
        oi.order_key,
        oi.customer_key,
        oi.part_key,
        oi.supplier_key,
        
        -- Date dimensions
        oi.order_date,
        (year(oi.order_date) * 10000 + month(oi.order_date) * 100 + day(oi.order_date)) as order_date_key,

        YEAR(oi.order_date) as order_year,
        QUARTER(oi.order_date) as order_quarter,
        MONTH(oi.order_date) as order_month,
        WEEK(oi.order_date) as order_week,
        DAYOFWEEK(oi.order_date) as order_day_of_week,
        TO_CHAR(oi.order_date, 'YYYY-MM') as order_month_name,
        TO_CHAR(oi.order_date, 'DY') as order_day_name,
        
        -- Customer attributes (denormalized for performance)
        c.customer_name,
        c.market_segment,
        c.customer_tier,
        c.balance_segment,
        c.region_name,
        c.nation_name,
        c.reporting_region,
        
        -- Product attributes (denormalized)
        p.part_name,
        p.brand,
        p.retail_price,
        
        -- Measures
        oi.quantity,
        CAST(extended_price AS NUMBER(18,2)) as extended_price,
        CAST(discount AS NUMBER(5,2)) as discount,
        oi.discount * 100 as discount_pct,
        oi.extended_price * (1 - oi.discount) as net_sales,
        
        -- Calculated measures
        oi.extended_price - (oi.extended_price * (1 - oi.discount)) as discount_amount,
        (oi.extended_price * (1 - oi.discount)) / NULLIF(oi.quantity, 0) as unit_price
        
    FROM {{ ref('int_orders_items') }} oi
    LEFT JOIN {{ ref('dim_customer') }} c 
        ON oi.customer_key = c.customer_key 
        AND c.is_current_record = TRUE
    LEFT JOIN {{ ref('dim_part') }} p 
        ON oi.part_key = p.part_key
)

SELECT * FROM fact_enriched