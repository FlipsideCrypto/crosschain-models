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
        p.id,
        symbol,
        decimals,
        price,
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
        p.blockchain_id,
        provider,
        is_imputed,
        CASE
            WHEN is_deprecated IS NULL THEN FALSE
            ELSE is_deprecated
        END AS is_deprecated,
        GREATEST(COALESCE(p.inserted_timestamp, '2000-01-01'), COALESCE(m.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
        GREATEST(COALESCE(p.modified_timestamp, '2000-01-01'), COALESCE(m.modified_timestamp, '2000-01-01')) AS modified_timestamp,
        token_prices_priority_hourly_id AS ez_hourly_token_prices_id
    FROM
        {{ ref('silver__token_prices_priority2') }}
        p
        LEFT JOIN {{ ref('price_temp__ez_asset_metadata_temp') }}
        m
        ON p.token_address = m.token_address
        AND p.blockchain_id = m.blockchain_id
)
SELECT
    HOUR,
    token_address,
    id,
    symbol,
    decimals,
    price,
    blockchain_adj AS blockchain,
    blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    inserted_timestamp,
    modified_timestamp,
    ez_hourly_token_prices_id
FROM
    base
