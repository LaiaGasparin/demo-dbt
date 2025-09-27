{{ config(materialized='table', unique_key='order_key', on_schema_change='sync_all_columns') }}

select
  l.order_key,
  o.customer_key,
  l.part_key,
  l.supplier_key,
  o.order_date,
  l.ship_date,   
  l.quantity,
  l.extended_price,
  l.discount
from {{ ref('stg_lineitem') }} l
join {{ ref('stg_orders') }}  o
  on l.order_key = o.order_key
