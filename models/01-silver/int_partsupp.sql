{{ config(materialized='table', unique_key='partsupp_key', on_schema_change='sync_all_columns') }}

select
  -- natural keys and fk
  part_key,
  supplier_key,

  supply_cost

from {{ ref('stg_partsupp') }}