{{ config(materialized='table') }}
select 
    region_key, 
    region_name,
    reporting_region
from {{ ref('int_region') }}
