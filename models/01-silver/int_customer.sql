{{ config(
    materialized='table',
    tags=['silver', 'intermediate']
) }}

SELECT 
    -- Primary Key
    customer_key,
    customer_name,
    nation_key,  

    -- measures
    account_balance,
    market_segment,
    
    -- Value-add transformations 
    CASE 
        WHEN account_balance < 0 THEN 'Credit'
        WHEN account_balance = 0 THEN 'Zero Balance'
        WHEN account_balance <= 1000 THEN 'Low Balance'
        WHEN account_balance <= 5000 THEN 'Medium Balance'
        ELSE 'High Balance'
    END as balance_segment,
    
    CASE 
        WHEN account_balance > 7000 THEN 'Premium'
        WHEN account_balance > 3000 THEN 'Standard'
        ELSE 'Basic'
    END as customer_tier

FROM {{ ref('stg_customer') }} 