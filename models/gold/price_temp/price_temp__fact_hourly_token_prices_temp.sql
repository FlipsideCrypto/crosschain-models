{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

    SELECT
        HOUR,
        token_address,
        TRIM(REPLACE(REPLACE(blockchain, '-', ''), ' ', '')) AS blockchain_adj,
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
            WHEN blockchain_adj IN (
                'cosmos',
                'evmos',
                'osmosis',
                'terra',
                'terra2'
            ) THEN 'cosmos'
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
        blockchain_id,
        provider,
        price,
        is_imputed,
        inserted_timestamp,
        modified_timestamp,
        token_prices_all_providers_hourly_id AS fact_hourly_token_prices_id
    FROM
        {{ ref('silver__token_prices_all_providers3') }}
)
SELECT
    hour,
    token_address,
    blockchain,
    blockchain_id,
    provider,
    price,
    is_imputed,
    inserted_timestamp,
    modified_timestamp,
    fact_hourly_token_prices_id
FROM
    base