{{ config(
    materialized = 'incremental',
    unique_key = ['token_prices_all_providers_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE'],
    tags = ['prices']
) }}

WITH coin_gecko AS (

    SELECT
        recorded_hour,
        token_address,
        platform,
        platform_id,
        CLOSE AS price,
        is_imputed,
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
        recorded_hour,
        token_address,
        platform,
        platform_id,
        CLOSE AS price,
        is_imputed,
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
        recorded_hour,
        token_address,
        'cosmos' AS platform,
        'cosmos' AS platform_id,
        CLOSE AS price,
        is_imputed,
        id,
        price_source AS provider,
        'ibc_prices' AS source,
        _inserted_timestamp
    FROM
        {{ ref('silver__onchain_osmosis_prices2') }}

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
mapping AS (
    SELECT
        recorded_hour,
        CASE
            WHEN p.token_address ILIKE 'ibc%'
            OR platform = 'solana' THEN p.token_address
            ELSE LOWER(
                p.token_address
            )
        END AS token_address,
        TRIM(REPLACE(platform, '-', ' ')) AS platform_adj,
        CASE
            WHEN platform IN (
                'arbitrum',
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
            ELSE platform_adj
        END AS blockchain,
        platform AS blockchain_name,
        platform_id AS blockchain_id,
        price,
        is_imputed,
        id,
        provider,
        source,
        _inserted_timestamp
    FROM
        all_providers p
)
SELECT
    recorded_hour,
    token_address,
    blockchain,
    blockchain_name,
    blockchain_id,
    price,
    is_imputed,
    id,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['recorded_hour','LOWER(token_address)','blockchain_id','provider']) }} AS token_prices_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    mapping qualify(ROW_NUMBER() over (PARTITION BY recorded_hour, LOWER(token_address), blockchain_id, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
