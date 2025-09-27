{{ config(materialized='table') }}

select
  part_key,          -- PK for joins from fact_sales
  part_name,
  brand,
  part_type,
  part_size,
  container,
  retail_price
from {{ ref('int_part') }}
