{{ config(materialized="view") }}

select
    l_orderkey as order_key,
    l_partkey as part_key,
    l_suppkey as supplier_key,
    l_quantity as quantity,
    l_extendedprice as extended_price,
    l_discount as discount
from {{ source("tpch", "lineitem") }}
