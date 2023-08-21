{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('core__dim_tags') }}
