{{ config(
    materialized = 'view',
) }}

SELECT
    'coingecko' AS provider,
    id,
    token_address,
    NAME,
    upper(symbol) as symbol,
    platform
FROM
    {{ ref('silver__asset_metadata_coin_gecko') }}
UNION 
SELECT
    'coinmarketcap' AS provider,
    id,
    token_address,
    NAME,
    upper(symbol) as symbol,
    platform
FROM
    {{ ref('silver__asset_metadata_coin_market_cap') }}
