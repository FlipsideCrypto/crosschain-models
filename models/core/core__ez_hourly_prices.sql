{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT 
    hour,
    p.token_address,
    symbol,
    decimals,
    price,
    p.blockchain,
    is_imputed
FROM {{ ref('silver__token_prices_priority_hourly') }} p
LEFT JOIN {{ ref('core__ez_asset_metadata') }} m
    ON p.token_address = m.token_address 
        AND p.blockchain = m.blockchain