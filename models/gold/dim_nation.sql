{{ config(materialized='table') }}
select 
    nation_key, 
    nation_name, 
    region_key
from {{ ref('int_nation') }}
