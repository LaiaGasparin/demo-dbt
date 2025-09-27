{{ config(materialized='view') }}

SELECT 
    region_key,
    region_name,
    CASE region_name
        WHEN 'AMERICA' THEN 'AMER'
        WHEN 'EUROPE' THEN 'EMEA'
        WHEN 'MIDDLE EAST' THEN 'EMEA'
        WHEN 'AFRICA' THEN 'EMEA'
        WHEN 'ASIA' THEN 'APAC'
    END as reporting_region
FROM {{ ref('stg_region') }}
