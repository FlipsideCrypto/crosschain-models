{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

SELECT 
    hour,
    p.token_address,
    COALESCE(cg.symbol,cmc.symbol) AS symbol,
    COALESCE(cg.decimals,cmc.decimals) AS decimals,
    price,
    p.blockchain,
    is_imputed,
    _inserted_timestamp,
    _unique_key
FROM {{ ref('silver__token_prices_priority_hourly') }} p
LEFT JOIN (
    SELECT 
        token_address,
        symbol,
        decimals,
        blockchain,
        provider
    FROM {{ ref('core__dim_asset_metadata') }}
    WHERE provider = 'coingecko'
        ) cg 
    ON p.token_address = cg.token_address AND p.blockchain = cg.blockchain
LEFT JOIN (
    SELECT
        token_address,
        symbol,
        decimals,
        blockchain,
        provider
    FROM {{ ref('core__dim_asset_metadata') }}
    WHERE provider = 'coinmarketcap'
        ) cmc 
    ON p.token_address = cmc.token_address AND p.blockchain = cmc.blockchain
QUALIFY(ROW_NUMBER() OVER (PARTITION BY hour, p.token_address, p.blockchain 
    ORDER BY COALESCE(cg.symbol,cmc.symbol) ASC)) = 1
)

SELECT
    hour,
    token_address,
    symbol,
    decimals,
    price,
    blockchain
FROM base
