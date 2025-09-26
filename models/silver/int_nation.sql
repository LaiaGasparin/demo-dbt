{{ config(materialized='table') }}
select
  n.nation_key,
  n.nation_name,
  n.region_key
from {{ ref('stg_nation') }} n


