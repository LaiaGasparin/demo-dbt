{{ config(materialized='table') }}
select
  c.customer_key,
  c.customer_name,
  n.nation_name,
  r.region_name,
  c.account_balance
from {{ ref('stg_customer') }} c
join {{ ref('int_nation') }}  n on c.nation_key = n.nation_key
join {{ ref('int_region') }}  r on n.region_key = r.region_key
