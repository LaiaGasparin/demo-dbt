{{ config( materialized='view')}}

select 
   ps_partkey as part_key,
   ps_suppkey as supplier_key,
   ps_supplycost as supply_cost

from {{ source('tpch','partsupp')}}