{{ config(materialized='table', on_schema_change='sync_all_columns') }}

SELECT
  -- order
  oi.order_part_sk,
  oi.order_key,


  -- location
  c.nation_key,
  n.region_key,
  r.reporting_region,

  -- customer
  oi.customer_key,
  c.market_segment,
  c.customer_tier,
  c.balance_segment,

  -- product
  oi.part_key,
  p.is_promo,

  -- date
  oi.order_date_key,
  oi.order_date,
  d.date_day,
  DATE_TRUNC('month', d.date_day)   AS date_month, -- if DATE_DAY = '2025-09-27', then month_start = '2025-09-01' - to be added in the dim_date
  d.quarter,
  d.year,

  -- supplier
  oi.supplier_key,

  -- amounts
  oi.quantity,
  oi.extended_price_with_discount,
  oi.extended_cost,
  oi.extended_net,
  oi.discount



FROM {{ ref('dim_order_item') }} oi
LEFT JOIN {{ ref('dim_date') }}     d ON oi.order_date_key = d.date_key
LEFT JOIN {{ ref('dim_customer') }} c ON oi.customer_key   = c.customer_key 
LEFT JOIN {{ ref('dim_part') }}     p ON oi.part_key       = p.part_key
LEFT JOIN {{ ref('dim_partsupp')}} ps ON oi.supplier_key = ps.supplier_key AND oi.part_key = ps.part_key
LEFT JOIN {{ ref('dim_nation') }}   n  ON c.nation_key = n.nation_key
LEFT JOIN {{ ref('dim_region') }}   r  ON n.region_key = r.region_key
