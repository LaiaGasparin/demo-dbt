{{ config(materialized='table') }}

SELECT 
    c.customer_key,
    c.customer_name,
    c.nation_key,
    n.nation_name,
    n.region_key,
    r.region_name,
    c.account_balance,
    c.market_segment
FROM {{ ref('stg_customer') }} c
LEFT JOIN {{ ref('stg_nation') }} n ON c.nation_key = n.nation_key
LEFT JOIN {{ ref('stg_region') }} r ON n.region_key = r.region_key
