{{ config(materialized='view') }}

SELECT 
    nation_key,
    nation_name,
    region_key,
    TRIM(UPPER(nation_name)) as nation_code
FROM {{ ref('stg_nation') }}