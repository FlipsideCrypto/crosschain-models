{{ config(
    materialized = 'incremental',
    unique_key = ['hour','token_address','blockchain','provider'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['hour::DATE']
) }}

WITH coin_gecko AS (

    SELECT
        recorded_hour AS HOUR,
        token_address,
        platform,
        CLOSE AS price,
        imputed AS is_imputed,
        id,
        'coingecko' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_coingecko2') }}

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
        recorded_hour AS HOUR,
        token_address,
        platform,
        CLOSE AS price,
        imputed AS is_imputed,
        id,
        'coinmarketcap' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_coinmarketcap2') }}

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
ibc_prices AS (
    SELECT
        recorded_hour AS HOUR,
        token_address,
        'cosmos' AS platform,
        CLOSE AS price,
        is_imputed,
        id,
        price_source AS provider,
        'ibc_prices' AS source,
        _inserted_timestamp
    FROM
        {{ ref('silver__onchain_osmosis_prices') }}

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
        ibc_prices
),
supported_chains AS (
    SELECT
        HOUR,
        token_address,
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
        price,
        is_imputed,
        id,
        provider,
        source,
        _inserted_timestamp
    FROM
        all_providers p
    WHERE
        blockchain IS NOT NULL
),
FINAL AS (
    SELECT
        HOUR,
        token_address,
        blockchain,
        price,
        is_imputed,
        id,
        provider,
        source,
        _inserted_timestamp
    FROM
        supported_chains
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
    HOUR,
    token_address,
    blockchain,
    price,
    is_imputed,
    id,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['hour','token_address','blockchain','provider']) }} AS token_prices_all_providers_hourly_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY HOUR, token_address, blockchain, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
