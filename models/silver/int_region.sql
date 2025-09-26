{{ config(materialized='table') }}

select
  region_key,
  region_name
from {{ ref('stg_region') }}
