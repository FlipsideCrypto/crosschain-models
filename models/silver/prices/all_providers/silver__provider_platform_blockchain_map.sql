{{ config(
    materialized = 'view',
    tags = ['daily']
) }}

WITH coin_gecko AS (

    SELECT
        DISTINCT platform,
        platform_id,
        'coingecko' AS provider
    FROM
        {{ ref(
            'silver__token_asset_metadata_coingecko'
        ) }}
),
coin_market_cap AS (
    SELECT
        DISTINCT platform,
        platform_id,
        'coinmarketcap' AS provider
    FROM
        {{ ref(
            'silver__token_asset_metadata_coinmarketcap'
        ) }}
),
all_providers AS (
    SELECT
        *
    FROM
        coin_gecko
    UNION ALL
    SELECT
        *
    FROM
        coin_market_cap
)
SELECT
    platform,
    platform_id,
    provider,
    TRIM(REPLACE(platform, '-', ' ')) AS platform_adj,
    CASE
        WHEN platform IN (
            'arbitrum',
            'arbitrum-one'
        ) THEN 'arbitrum'
        WHEN platform IN (
            'avalanche',
            'avalanche c-chain'
        ) THEN 'avalanche'
        WHEN platform IN (
            'binance-smart-chain',
            'binancecoin',
            'bnb'
        ) THEN 'bsc'
        WHEN platform IN (
            'bob network',
            'bob-network'
        ) THEN 'bob'
        WHEN platform = 'boba network' THEN 'boba'
        WHEN platform IN (
            'gnosis',
            'xdai',
            'gnosis chain'
        ) THEN 'gnosis'
        WHEN platform IN (
            'optimism',
            'optimistic-ethereum'
        ) THEN 'optimism'
        WHEN platform IN (
            'polygon',
            'polygon-pos'
        ) THEN 'polygon'
        WHEN platform IN(
            'toncoin',
            'the open network'
        ) THEN 'ton'
        WHEN platform_adj IN (
            'sei network',
            'sei v2'
        ) THEN 'sei'
        WHEN platform = 'flow evm' THEN 'flow'
        WHEN platform_adj = 'near protocol' THEN 'near'
        WHEN platform_adj = 'sui network' THEN 'sui'
        WHEN platform_adj = 'swellchain' THEN 'swell'
        ELSE platform_adj
    END AS blockchain,
    platform AS blockchain_name,
    platform_id AS blockchain_id,
FROM
    all_providers
