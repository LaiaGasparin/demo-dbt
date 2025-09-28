{{ config(
    materialized='table',
    unique_key='customer_key',
    on_schema_change='sync_all_columns',
    tags=['gold', 'dimension']
) }}

SELECT * FROM {{ ref('int_customer') }}
