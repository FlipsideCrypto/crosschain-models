{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base_priority AS (

    SELECT
        CASE
            WHEN A.token_address ILIKE 'ibc%'
            OR A.blockchain = 'solana' THEN A.token_address
            ELSE LOWER(
                A.token_address
            )
        END AS token_address,
        id,
        symbol,
        NAME,
        A.blockchain AS blockchain_name,
        CASE
            WHEN A.blockchain IN (
                'arbitrum',
                'arbitrum nova',
                'arbitrum-nova',
                'arbitrum-one'
            ) THEN 'arbitrum'
            WHEN A.blockchain IN (
                'avalanche',
                'avalanche c-chain'
            ) THEN 'avalanche'
            WHEN A.blockchain IN (
                'binance-smart-chain',
                'binancecoin',
                'bnb'
            ) THEN 'bsc'
            WHEN A.blockchain IN (
                'bitcoin',
                'bitcoin sv'
            ) THEN 'bitcoin'
            WHEN A.blockchain IN (
                'gnosis',
                'xdai',
                'gnosis chain'
            ) THEN 'gnosis'
            WHEN A.blockchain IN (
                'optimism',
                'optimistic-ethereum'
            ) THEN 'optimism'
            WHEN A.blockchain IN (
                'polygon',
                'polygon-pos'
            ) THEN 'polygon'
            WHEN A.blockchain IN (
                'cosmos',
                'evmos',
                'osmosis',
                'terra',
                'terra2'
            ) THEN 'cosmos'
            ELSE A.blockchain
        END AS blockchain_adj,
        blockchain_id,
        priority,
        is_deprecated,
        inserted_timestamp,
        modified_timestamp,
        token_asset_metadata_priority_id AS ez_asset_metadata_id
    FROM
        {{ ref('silver__token_asset_metadata_priority2') }} A
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
    blockchain_adj AS blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    GREATEST(COALESCE(s.inserted_timestamp, '2000-01-01'), COALESCE(C.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(s.modified_timestamp, '2000-01-01'), COALESCE(C.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    ez_asset_metadata_id
FROM
    base_priority s
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON LOWER(
        C.address
    ) = LOWER(
        s.token_address
    )
    AND C.blockchain = s.blockchain_adj qualify(ROW_NUMBER() over (PARTITION BY LOWER(token_address), blockchain_id
ORDER BY
    priority ASC)) = 1
