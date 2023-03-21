{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    p.hour,
    p.token_address,
    COALESCE(cg.symbol,cmc.symbol) AS symbol,
    COALESCE(cg.decimals,cmc.decimals) AS decimals,
    p.price,
    p.blockchain
FROM {{ ref('silver__token_prices_priority_hourly') }} p
LEFT JOIN (
    SELECT token_address, symbol, decimals
    FROM {{ ref('silver__asset_metadata_all_providers') }} 
    WHERE provider = 'coingecko'
        ) cg 
    ON p.token_address = cg.token_address
LEFT JOIN (
    SELECT token_address, symbol, decimals 
    FROM {{ ref('silver__asset_metadata_all_providers') }} 
    WHERE provider = 'coinmarketcap'
        ) cmc 
    ON p.token_address = cmc.token_address
