{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['hour::DATE'],
) }}

WITH all_providers AS (

    SELECT
        recorded_hour AS HOUR,
        LOWER(token_address) AS token_address,
        LOWER(REGEXP_REPLACE(platform, '[^a-zA-Z0-9/-]+')) AS platform,
        'coingecko' AS provider,
        CLOSE AS price,
        imputed AS is_imputed,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_coin_gecko_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    recorded_hour AS HOUR,
    LOWER(token_address) AS token_address,
    LOWER(REGEXP_REPLACE(platform, '[^a-zA-Z0-9/-]+')) AS platform,
    'coinmarketcap' AS provider,
    CLOSE AS price,
    imputed AS is_imputed,
    _inserted_timestamp
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    recorded_hour AS HOUR,
    token_address AS token_address,
    'cosmos' AS platform,
    source AS provider,
    CLOSE AS price,
    is_imputed,
    _inserted_timestamp
FROM
    {{ ref('silver__onchain_osmosis_prices') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        HOUR,
        token_address,
        CASE
            WHEN platform IN (
                'arbitrum-nova',
                'arbitrum-one',
                'arbitrum'
            ) THEN 'arbitrum'
            WHEN platform IN ('avalanche') THEN 'avalanche'
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
            WHEN platform IN (
                'cosmos',
                'evmos',
                'osmosis',
                'terra',
                'terra-2'
            ) THEN 'cosmos'
            WHEN LOWER(platform) = 'algorand' THEN 'algorand'
            ELSE NULL
        END AS blockchain,
        --supported chains only
        provider,
        price,
        is_imputed,
        _inserted_timestamp,
        {{ dbt_utils.surrogate_key(
            ['hour','token_address','blockchain','provider']
        ) }} AS _unique_key
    FROM
        all_providers p
    WHERE
        blockchain IS NOT NULL
)
SELECT
    HOUR,
    token_address,
    blockchain,
    provider,
    price,
    is_imputed,
    _inserted_timestamp,
    _unique_key
FROM
    FINAL
WHERE
    len(token_address) > 0 qualify(ROW_NUMBER() over (PARTITION BY HOUR, token_address, blockchain, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
