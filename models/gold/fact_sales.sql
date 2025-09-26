{{ config(materialized='table', on_schema_change='sync_all_columns') }}

select
  oi.order_key,
  oi.customer_key,
  oi.part_key,
  oi.supplier_key,
  oi.order_date,
  oi.quantity,
  oi.extended_price,
  oi.discount,
  (oi.extended_price * (1 - oi.discount)) as net_sales
from {{ ref('int_orders_items') }} oi
