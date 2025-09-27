{{ config(materialized='table') }}

select
  part_key,
  part_name,
  manufacturer,
  brand,
  part_type,
  part_size,
  container,
  cast(retail_price as number(18,2)) as retail_price
from {{ ref('stg_part') }}
