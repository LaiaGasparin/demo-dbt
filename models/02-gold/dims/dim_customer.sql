{{ config(
    materialized='table',
    unique_key='customer_key',
    on_schema_change='sync_all_columns',
    cluster_by=['region_name', 'market_segment'],
    tags=['gold', 'dimension']
) }}

WITH customer_base AS (
    SELECT 
        customer_key,
        customer_name,
        nation_key,
        account_balance,
        market_segment
    FROM {{ ref('int_customer') }}
),

geography AS (
    SELECT 
        n.nation_key,
        n.nation_name,
        n.nation_code,
        r.region_key,
        r.region_name,
        r.reporting_region
    FROM {{ ref('int_nation') }} n
    LEFT JOIN {{ ref('int_region') }} r 
        ON n.region_key = r.region_key
),

customer_enriched AS (
    SELECT 
        -- Primary Key
        c.customer_key,
        
        -- Customer Attributes
        c.customer_name,

        
        -- Geographic Hierarchy (denormalized for performance)
        c.nation_key,
        g.nation_name,
        g.nation_code,
        g.region_key,
        g.region_name,
        g.reporting_region,
        
        -- Market Segmentation
        c.market_segment,
        CASE 
            WHEN c.account_balance < 0 THEN 'Credit'
            WHEN c.account_balance = 0 THEN 'Zero Balance'
            WHEN c.account_balance <= 1000 THEN 'Low Balance'
            WHEN c.account_balance <= 5000 THEN 'Medium Balance'
            ELSE 'High Balance'
        END as balance_segment,
        
        -- Financial Metrics
        c.account_balance,
        
        -- Customer Classification
        CASE 
            WHEN c.account_balance > 7000 THEN 'Premium'
            WHEN c.account_balance > 3000 THEN 'Standard'
            ELSE 'Basic'
        END as customer_tier,

        
        -- SCD Type 2 columns (future-proofing)
        CURRENT_TIMESTAMP() as valid_from,
        CAST('9999-12-31' AS DATE) as valid_to,
        TRUE as is_current_record,
        
        -- Audit columns
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at,
        '{{ var("dbt_version", "1.0.0") }}' as dbt_version,
        '{{ invocation_id }}' as batch_id
    FROM customer_base c
    LEFT JOIN geography g ON c.nation_key = g.nation_key
)

SELECT * FROM customer_enriched