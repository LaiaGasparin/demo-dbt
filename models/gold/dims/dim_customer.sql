{{ config(materialized='table') }}
select
  customer_key,
  customer_name,
  nation_name,
  region_name,
  account_balance
from {{ ref('int_customer') }}
