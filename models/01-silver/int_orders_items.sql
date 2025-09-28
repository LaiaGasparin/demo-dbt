{{ config(
    materialized='table', 
    unique_key='order_part_sk', 
    on_schema_change='sync_all_columns'
) }}

SELECT
    -- Stable numeric surrogate from (order_key, part_key)
    HASH(l.order_key, l.part_key) AS order_part_sk,

    -- Natural keys / FKs
    l.order_key,
    o.customer_key,
    l.part_key,
    l.supplier_key,

    -- Dates
    o.order_date,
    TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD')) AS order_date_key,

    -- Amounts
    CAST(l.quantity AS NUMBER(18,0)) AS quantity,
    ROUND(l.quantity * p.retail_price, 2) AS extended_unit_price,
    CAST(l.extended_price AS NUMBER(18,2)) AS extended_price, -- retail price * quantity, no discount
    CAST(l.discount AS NUMBER(9,4)) AS discount,
    ROUND(l.extended_price * (1 - l.discount), 2) AS extended_price_with_discount,
    ROUND(l.extended_price * l.discount, 2) AS discount_amount,
    ROUND((l.extended_price - (ps.supply_cost * l.quantity)), 2) AS extended_cost,
    ROUND((l.extended_price - (ps.supply_cost * l.quantity)) * (1 - l.discount), 2) AS extended_net

FROM {{ ref('stg_lineitem') }} l
JOIN {{ ref('stg_orders') }} o 
    ON l.order_key = o.order_key
JOIN {{ ref('stg_partsupp') }} ps 
    ON l.part_key = ps.part_key 
    AND l.supplier_key = ps.supplier_key  -- Composite key join
JOIN {{ ref('stg_part') }} p 
    ON l.part_key = p.part_key