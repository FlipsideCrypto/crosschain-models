{{ config(
    materialized = 'incremental',
    unique_key = ['token_address','blockchain','provider'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH coin_gecko AS (

    SELECT
        id,
        token_address,
        CASE
            WHEN LENGTH(NAME) <= 0 THEN NULL
            ELSE NAME
        END AS NAME,
        CASE
            WHEN LENGTH(symbol) <= 0 THEN NULL
            ELSE symbol
        END AS symbol,
        platform,
        'coingecko' AS provider,
        source,
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
        CASE
            WHEN LENGTH(NAME) <= 0 THEN NULL
            ELSE NAME
        END AS NAME,
        CASE
            WHEN LENGTH(symbol) <= 0 THEN NULL
            ELSE symbol
        END AS symbol,
        platform,
        'coinmarketcap' AS provider,
        source,
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
        address AS token_address,
        CASE
            WHEN LENGTH(label) <= 0 THEN NULL
            ELSE label
        END AS NAME,
        CASE
            WHEN LENGTH(project_name) <= 0 THEN NULL
            ELSE project_name
        END AS symbol,
        'cosmos' AS platform,
        'onchain' AS provider,
        'ibc_am' AS source,
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
        'solscan' AS provider,
        'solscan' AS source,
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
),
FINAL AS (
    SELECT
        token_address,
        id,
        NAME,
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
            WHEN platform = 'ethereum' THEN 'ethereum'
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
            WHEN platform = 'base' THEN 'base'
            WHEN platform = 'blast' THEN 'blast'
            WHEN platform IN (
                'cosmos',
                'evmos',
                'osmosis',
                'terra',
                'terra-2'
            ) THEN 'cosmos'
            WHEN platform = 'algorand' THEN 'algorand'
            WHEN platform = 'solana' THEN 'solana'
            WHEN platform = 'aptos' THEN 'aptos'
            ELSE NULL
        END AS blockchain,
        --supported chains only
        provider,
        source,
        _inserted_timestamp
    FROM
        all_providers
    WHERE
        blockchain IS NOT NULL
),
FINAL AS (
    SELECT
        token_address,
        id,
        NAME,
        symbol,
        blockchain,
        provider,
        source,
        _inserted_timestamp
    FROM
        FINAL
    WHERE
        NOT (
            blockchain IN (
                'arbitrum',
                'avalanche',
                'bsc',
                'ethereum',
                'gnosis',
                'optimism',
                'polygon',
                'base',
                'blast'
            )
            AND token_address NOT ILIKE '0x%'
        )
        AND NOT (
            blockchain = 'algorand'
            AND TRY_CAST(
                token_address AS INT
            ) IS NULL
        )
)
SELECT
    token_address,
    id,
    NAME,
    symbol,
    blockchain,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','blockchain','provider']) }} AS token_asset_metadata_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY token_address, blockchain, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
