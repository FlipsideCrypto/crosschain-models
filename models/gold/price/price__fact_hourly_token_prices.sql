{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    hour,
    token_address,
    blockchain,
    provider,
    price,
    is_imputed
FROM {{ ref('silver__token_prices_all_providers_hourly') }}



