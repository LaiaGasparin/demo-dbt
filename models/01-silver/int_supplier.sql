{{config(materialized="table")}}

select 
    supplier_key,
    supplier_name,
    nation_key
from {{ref('stg_supplier')}}