{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

    SELECT
        token_address,
        id,
        symbol,
        NAME,
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
        inserted_timestamp,
        modified_timestamp,
        token_asset_metadata_all_providers_id AS dim_asset_metadata_id
    FROM
        {{ ref('silver__token_asset_metadata_all_providers3') }}
)
SELECT
    token_address,
    id,
    COALESCE(
        C.symbol,
        s.symbol
    ) AS symbol,
    COALESCE(
        C.name,
        s.name
    ) AS NAME,
    decimals,
    s.blockchain,
    blockchain_id,
    provider,
    GREATEST(COALESCE(s.inserted_timestamp, '2000-01-01'), COALESCE(C.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(s.modified_timestamp, '2000-01-01'), COALESCE(C.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    dim_asset_metadata_id
FROM
    base s
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON LOWER(
        C.address
    ) = LOWER(
        s.token_address
    )
    AND C.blockchain = s.blockchain qualify(ROW_NUMBER() over (PARTITION BY token_address, blockchain_id, provider
ORDER BY
    GREATEST(COALESCE(s.inserted_timestamp, '2000-01-01'), COALESCE(C.inserted_timestamp, '2000-01-01')) DESC)) = 1
