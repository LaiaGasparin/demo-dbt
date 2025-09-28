{{ config(materialized='table') }}

select
  part_key,
  part_name,
  manufacturer,
  brand,
  part_type,
  case 
    when part_type like 'PROMO%' then true
    else false
  end as is_promo,
  cast(retail_price as number(18,2)) as retail_price
from {{ ref('stg_part') }}
