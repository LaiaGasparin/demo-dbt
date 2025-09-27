{{ config(materialized='table') }}
select region_key, region_name
from {{ ref('int_region') }}
