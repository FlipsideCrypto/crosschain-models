{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    hour,
    token_address,
    symbol,
    decimals,
    price,
    blockchain
FROM {{ ref('silver__token_prices_priority_hourly') }}
LEFT JOIN {{ ref('core_dim_asset_metadata') }} USING (token_address)

