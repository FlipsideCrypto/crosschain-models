{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

    SELECT
        HOUR,
        CASE
            WHEN p.token_address ILIKE 'ibc%'
            OR p.blockchain = 'solana' THEN p.token_address
            ELSE LOWER(
                p.token_address
            )
        END AS token_address,
        p.blockchain AS blockchain_name,
        CASE
            WHEN p.blockchain IN (
                'arbitrum',
                'arbitrum nova',
                'arbitrum-nova',
                'arbitrum-one'
            ) THEN 'arbitrum'
            WHEN p.blockchain IN (
                'avalanche',
                'avalanche c-chain'
            ) THEN 'avalanche'
            WHEN p.blockchain IN (
                'binance-smart-chain',
                'binancecoin',
                'bnb'
            ) THEN 'bsc'
            WHEN p.blockchain IN (
                'bitcoin',
                'bitcoin sv'
            ) THEN 'bitcoin'
            WHEN p.blockchain IN (
                'gnosis',
                'xdai',
                'gnosis chain'
            ) THEN 'gnosis'
            WHEN p.blockchain IN (
                'optimism',
                'optimistic-ethereum'
            ) THEN 'optimism'
            WHEN p.blockchain IN (
                'polygon',
                'polygon-pos'
            ) THEN 'polygon'
            WHEN p.blockchain IN (
                'cosmos',
                'evmos',
                'osmosis',
                'terra',
                'terra2'
            ) THEN 'cosmos'
            ELSE p.blockchain
        END AS blockchain_adj,
        blockchain_id,
        provider,
        price,
        is_imputed,
        inserted_timestamp,
        modified_timestamp,
        token_prices_all_providers_hourly_id AS fact_hourly_token_prices_id
    FROM
        {{ ref('silver__token_prices_all_providers2') }} p
)
SELECT
    hour,
    token_address,
    blockchain_adj AS blockchain,
    blockchain_name,
    blockchain_id,
    provider,
    price,
    is_imputed,
    inserted_timestamp,
    modified_timestamp,
    fact_hourly_token_prices_id
FROM
    base