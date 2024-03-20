{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

    SELECT
        HOUR,
        p.token_address,
        symbol,
        decimals,
        price,
        TRIM(REPLACE(REPLACE(p.blockchain, '-', ''), ' ', '')) AS blockchain_adj,
        CASE
            WHEN blockchain_adj IN (
                'arbitrumnova',
                'arbitrumone',
                'arbitrum'
            ) THEN 'arbitrum'
            WHEN blockchain_adj IN (
                'avalanche',
                'avalanchecchain'
            ) THEN 'avalanche'
            WHEN blockchain_adj IN (
                'binancesmartchain',
                'binancecoin',
                'bnb'
            ) THEN 'bsc'
            WHEN blockchain_adj = 'ethereum' THEN 'ethereum'
            WHEN blockchain_adj IN (
                'gnosis',
                'xdai',
                'gnosischain'
            ) THEN 'gnosis'
            WHEN blockchain_adj IN (
                'optimism',
                'optimisticethereum'
            ) THEN 'optimism'
            WHEN blockchain_adj IN (
                'polygon',
                'polygonpos'
            ) THEN 'polygon'
            WHEN blockchain_adj = 'base' THEN 'base'
            WHEN blockchain_adj = 'blast' THEN 'blast'
            WHEN blockchain_adj IN (
                'cosmos',
                'evmos',
                'osmosis',
                'terra',
                'terra2'
            ) THEN 'cosmos'
            WHEN blockchain_adj = 'algorand' THEN 'algorand'
            WHEN blockchain_adj = 'solana' THEN 'solana'
            WHEN blockchain_adj = 'aptos' THEN 'aptos'
            WHEN blockchain_adj IN (
                'bnbbeaconchain(bep2)',
                'bnbsmartchain(bep20)'
            ) THEN 'bnb beacon chain'
            WHEN blockchain_adj IN (
                'bobanetwork',
                'boba'
            ) THEN 'boba'
            WHEN blockchain_adj IN (
                'dogechain',
                'dogechain(evm)'
            ) THEN 'dogechain'
            WHEN blockchain_adj IN (
                'eosevm',
                'eos'
            ) THEN 'eos'
            WHEN blockchain_adj IN (
                'harmony',
                'harmonyshard0'
            ) THEN 'harmony'
            WHEN blockchain_adj IN (
                'hoo',
                'hoosmartchain'
            ) THEN 'hoo'
            WHEN blockchain_adj IN (
                'kadena',
                'kadenachain'
            ) THEN 'kadena'
            WHEN blockchain_adj IN (
                'milkomeda',
                'milkomedacardano'
            ) THEN 'milkomeda'
            WHEN blockchain_adj IN (
                'near',
                'nearprotocol'
            ) THEN 'near'
            WHEN blockchain_adj IN (
                'oasis',
                'oasisnetwork'
            ) THEN 'oasis'
            WHEN blockchain_adj IN (
                'ordinals',
                'ordinalsbrc20'
            ) THEN 'ordinals'
            WHEN blockchain_adj IN (
                'songbird',
                'songbirdnetwork'
            ) THEN 'songbird'
            ELSE blockchain_adj
        END AS blockchain,
        p.blockchain_id AS blockchain_id,
        is_imputed,
        GREATEST(COALESCE(p.inserted_timestamp, '2000-01-01'), COALESCE(m.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
        GREATEST(COALESCE(p.modified_timestamp, '2000-01-01'), COALESCE(m.modified_timestamp, '2000-01-01')) AS modified_timestamp,
        token_prices_priority_hourly_id AS ez_hourly_token_prices_id
    FROM
        {{ ref('silver__token_prices_priority3') }}
        p
        LEFT JOIN {{ ref('price_temp__ez_asset_metadata_temp') }}
        m
        ON p.token_address = m.token_address
        AND p.blockchain_id = m.blockchain_id
)
SELECT
    hour,
    token_address,
    symbol,
    decimals,
    price,
    blockchain,
    blockchain_id,
    is_imputed,
    inserted_timestamp,
    modified_timestamp,
    ez_hourly_token_prices_id
FROM
    base