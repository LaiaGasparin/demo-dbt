{{ config(materialized='table', on_schema_change='sync_all_columns') }}

SELECT
  supplier_key,
  supplier_name,
  nation_key
from {{ref('int_supplier')}}