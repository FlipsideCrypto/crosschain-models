{{ config(
    materialized = 'incremental',
    unique_key = ['token_address','blockchain_id','provider'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH coin_gecko AS (

    SELECT
        id,
        token_address,
        NAME,
        symbol,
        platform,
        platform_id,
        'coingecko' AS provider,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__token_asset_metadata_coingecko2'
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
coin_market_cap AS (
    SELECT
        id,
        token_address,
        NAME,
        symbol,
        platform,
        platform_id,
        'coinmarketcap' AS provider,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__token_asset_metadata_coinmarketcap2'
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
ibc_am AS (
    SELECT
        address AS id,
        raw_metadata [0] :denom :: STRING AS token_address,
        CASE
            WHEN LENGTH(label) <= 0 THEN NULL
            ELSE label
        END AS NAME,
        CASE
            WHEN LENGTH(project_name) <= 0 THEN NULL
            ELSE project_name
        END AS symbol,
        'cosmos' AS platform,
        'cosmos' AS platform_id,
        'osmosis-onchain' AS provider,
        'ibc_am' AS source,
        FALSE AS is_deprecated,
        '2000-01-01' :: TIMESTAMP AS _inserted_timestamp
    FROM
        {{ source(
            'osmosis_silver',
            'asset_metadata'
        ) }}
    WHERE
        address IS NOT NULL
        AND LENGTH(address) > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
solana_solscan AS (
    SELECT
        LOWER(
            CASE
                WHEN LENGTH(coingecko_id) <= 0
                OR coingecko_id IS NULL THEN token_address
                ELSE coingecko_id
            END
        ) AS id,
        token_address,
        CASE
            WHEN LENGTH(NAME) <= 0 THEN NULL
            ELSE NAME
        END AS NAME,
        CASE
            WHEN LENGTH(symbol) <= 0 THEN NULL
            ELSE symbol
        END AS symbol,
        'solana' AS platform,
        'solana' AS platform_id,
        'solscan' AS provider,
        'solscan' AS source,
        FALSE AS is_deprecated,
        _inserted_timestamp
    FROM
        {{ source(
            'solana_silver',
            'solscan_tokens'
        ) }}
    WHERE
        token_address IS NOT NULL
        AND LENGTH(token_address) > 0

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
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
)
SELECT
    CASE
        WHEN p.token_address ILIKE 'ibc%'
        OR platform = 'solana' THEN p.token_address
        ELSE LOWER(
            p.token_address
        )
    END AS token_address,
    id,
    NAME,
    symbol,
    CASE
        WHEN platform IN (
            'arbitrum',
            'arbitrum nova',
            'arbitrum-nova',
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
            'bitcoin',
            'bitcoin sv'
        ) THEN 'bitcoin'
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
        WHEN platform IN (
            'cosmos',
            'evmos',
            'osmosis',
            'terra',
            'terra2'
        ) THEN 'cosmos'
        ELSE platform
    END AS blockchain,
    platform AS blockchain_name,
    platform_id AS blockchain_id,
    provider,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','blockchain_id','provider']) }} AS token_asset_metadata_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers p qualify(ROW_NUMBER() over (PARTITION BY token_address, blockchain_id, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
