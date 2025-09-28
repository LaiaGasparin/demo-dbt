{{ config(materialized='table') }}

select
  part_key,          -- PK for joins from fact_sales
  part_name,
  brand,
  part_type,
  retail_price,
  is_promo
from {{ ref('int_part') }}
