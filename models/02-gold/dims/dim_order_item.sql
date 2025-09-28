{{ config(
    materialized='table',
    unique_key='order_part_sk',
    on_schema_change='sync_all_columns',
    cluster_by=['order_date_key', 'customer_key'],
    tags=['gold', 'fact', 'kimball']
) }}

SELECT
    -- Surrogate key
    order_part_sk,
    
    -- Foreign keys to dimensions
    order_key,
    part_key,
    customer_key,
    supplier_key,
    
    -- Date dimension key
    order_date_key,
    order_date,
    
    -- Measures
    quantity,
    extended_unit_price,              -- Added: retail price * quantity
    extended_price,                    -- Original line item price
    discount,                          -- Discount percentage
    extended_price_with_discount,     -- Price after discount
    discount_amount,                   -- Dollar amount of discount
    extended_cost,                     -- Added: profit margin (price - supply cost)
    extended_net                       -- Added: net profit after discount

FROM {{ ref('int_orders_items') }}