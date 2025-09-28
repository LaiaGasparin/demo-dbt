{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

SELECT
    part_key,
    supplier_key,
    supply_cost

FROM {{ ref('int_partsupp') }}
