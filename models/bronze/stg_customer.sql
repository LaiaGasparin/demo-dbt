{{ config(materialized='view') }}

select
  c_custkey   as customer_key,
  c_name      as customer_name,
  c_nationkey as nation_key,
  c_acctbal   as account_balance
from {{ source('tpch','customer') }}
