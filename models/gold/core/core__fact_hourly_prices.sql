{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    *
FROM
    {{ ref('price__fact_hourly_token_prices') }}
