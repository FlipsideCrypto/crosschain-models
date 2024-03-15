{{ config(
    materialized = 'incremental',
    unique_key = "asset_metadata_all_providers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
) }}
--delete+insert?
--incremental logic in CTES?
WITH coin_gecko AS (

    SELECT
        token_address,
        id,
        symbol,
        platform,
        'coingecko' AS provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__token_asset_metadata_coingecko2'
        ) }}
),
coin_market_cap AS (

    SELECT
        token_address,
        id,
        symbol,
        platform,
        'coinmarketcap' AS provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__token_asset_metadata_coinmarketcap2'
        ) }}
),
ibc_am AS (
    SELECT
        address AS token_address,
        address AS id,
        project_name AS symbol,
        'cosmos' AS platform,
        'onchain' AS provider,
        '2000-01-01' :: TIMESTAMP AS _inserted_timestamp
    FROM
        {{ source(
            'osmosis_silver',
            'asset_metadata'
        ) }}
    WHERE address IS NOT NULL
    AND LENGTH(address) > 0
),
solana_solscan AS (
    SELECT
        token_address,
        LOWER(COALESCE(coingecko_id, token_address)) AS id,
        symbol,
        'solana' AS platform,
        'solscan' AS provider,
        _inserted_timestamp
    FROM
        {{ source(
            'solana_silver',
            'solscan_tokens'
        ) }}
    WHERE token_address IS NOT NULL
    AND LENGTH(token_address) > 0
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
    UNION ALL
    SELECT
        *
    FROM
        ibc_am
    UNION ALL
    SELECT
        *
    FROM
        solana_solscan
),
FINAL AS (
    SELECT
        token_address,
        id,
        symbol,
        CASE
            WHEN platform IN (
                'arbitrum-nova',
                'arbitrum-one',
                'arbitrum'
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
            WHEN platform IN ('ethereum') THEN 'ethereum'
            WHEN platform IN (
                'gnosis',
                'xdai'
            ) THEN 'gnosis'
            WHEN platform IN (
                'optimism',
                'optimistic-ethereum'
            ) THEN 'optimism'
            WHEN platform IN (
                'polygon',
                'polygon-pos'
            ) THEN 'polygon'
            WHEN LOWER(platform) IN ('base') THEN 'base'
            WHEN LOWER(platform) IN ('blast') THEN 'blast'
            WHEN platform IN (
                'cosmos',
                'evmos',
                'osmosis',
                'terra',
                'terra-2'
            ) THEN 'cosmos'
            WHEN LOWER(platform) = 'algorand' THEN 'algorand'
            WHEN LOWER(platform) = 'solana' THEN 'solana'
            WHEN LOWER(platform) = 'aptos' THEN 'aptos'
            ELSE NULL
        END AS blockchain,
        --supported chains only
        provider,
        _inserted_timestamp
    FROM
        all_sources
    WHERE
        blockchain IS NOT NULL

{% if is_incremental() %}
AND token_address || blockchain NOT IN (
    SELECT
        DISTINCT token_address || blockchain
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    token_address,
    id,
    symbol,
    blockchain,
    provider,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','id','symbol','blockchain','provider']) }} AS asset_metadata_all_providers_id, --rework this id to be unique on token_address rather than id
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    NOT (LOWER(blockchain) IN ('arbitrum', 'avalanche', 'bsc', 'ethereum', 'gnosis', 'optimism', 'polygon', 'base', 'blast')
    AND token_address NOT ILIKE '0x%')
    AND NOT (
        blockchain = 'algorand'
        AND TRY_CAST(
            token_address AS INT
        ) IS NULL
    ) --rework this where filter
qualify(ROW_NUMBER() over (PARTITION BY token_address, id, symbol, blockchain, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
