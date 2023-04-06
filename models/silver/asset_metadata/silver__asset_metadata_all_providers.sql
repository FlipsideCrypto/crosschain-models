{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge'
) }}

WITH coin_gecko_meta AS (
    SELECT
        DISTINCT REGEXP_SUBSTR(REGEXP_REPLACE(token_address, '^x', '0x'), '0x[a-zA-Z0-9]*') AS token_address,
        LOWER(id) AS id,
        LOWER(symbol) AS symbol,
        LOWER(platform :: STRING) AS platform,
        'coingecko' AS provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__asset_metadata_coin_gecko'
        ) }} 
{% if is_incremental() %}
WHERE
    token_address NOT IN (
        SELECT 
            DISTINCT token_address
        FROM {{ this }}
    )
{% endif %}
),

coin_market_cap_meta AS (
    SELECT
        DISTINCT REGEXP_SUBSTR(REGEXP_REPLACE(token_address, '^x', '0x'), '0x[a-zA-Z0-9]*') AS token_address,
        LOWER(id) AS id,
        LOWER(symbol) AS symbol,
        LOWER(platform :: STRING) AS platform,
        'coinmarketcap' AS provider,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__asset_metadata_coin_market_cap'
        ) }}
{% if is_incremental() %}
WHERE
    token_address NOT IN (
        SELECT 
            DISTINCT token_address
        FROM {{ this }}
    )
{% endif %}
),

legacy_coin_gecko_meta AS (
    SELECT
        m.token_address AS token_address,
        LOWER(p.asset_id) AS id,
        LOWER(COALESCE(p.symbol,m.symbol)) AS symbol,
        LOWER(p.platform :name :: STRING) AS platform,
        p.provider,
        _inserted_timestamp
    FROM
        {{ source(
            'legacy_db',
            'prices_v2'
        ) }} p
        LEFT JOIN coin_gecko_meta m
        ON m.id = p.asset_id
    WHERE
        p.provider = 'coingecko'
        AND recorded_at :: DATE < '2022-08-24'
        AND m.token_address IS NOT NULL
        AND p.platform IS NOT NULL
{% if is_incremental() %}
AND
    m.token_address NOT IN (
        SELECT 
            DISTINCT token_address
        FROM {{ this }}
    )
{% endif %}
),

legacy_coin_market_cap_meta AS (
    SELECT
        m.token_address AS token_address,
        LOWER(p.asset_id) AS id,
        LOWER(COALESCE(p.symbol,m.symbol)) AS symbol,
        LOWER(p.platform :name :: STRING) AS platform,
        p.provider,
        _inserted_timestamp
    FROM
        {{ source(
            'legacy_db',
            'prices_v2'
        ) }} p
        LEFT JOIN coin_market_cap_meta m
        ON m.id = p.asset_id
    WHERE
        p.provider = 'coinmarketcap'
        AND recorded_at :: DATE < '2022-07-20'
        AND m.token_address IS NOT NULL
        AND p.platform IS NOT NULL
{% if is_incremental() %}
AND
    m.token_address NOT IN (
        SELECT 
            DISTINCT token_address
        FROM {{ this }}
    )
{% endif %}
),

all_sources AS (
    SELECT
        LOWER(token_address) AS token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        coin_gecko_meta
    UNION
    SELECT
        LOWER(token_address) AS token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        coin_market_cap_meta
    UNION
    SELECT
        LOWER(token_address) AS token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        legacy_coin_gecko_meta
    UNION
    SELECT
        LOWER(token_address) AS token_address,
        id,
        symbol,
        platform,
        provider,
        _inserted_timestamp
    FROM
        legacy_coin_market_cap_meta
),

FINAL AS (

SELECT
    token_address,
    id,
    symbol,
    CASE 
        WHEN platform IN ('arbitrum-nova','arbitrum-one','arbitrum') THEN 'arbitrum'
        WHEN platform IN ('avalanche') THEN 'avalanche'
        WHEN platform IN ('binance-smart-chain','binancecoin','bnb') THEN 'bsc'
        WHEN platform IN ('ethereum') THEN 'ethereum'
        WHEN platform IN ('gnosis','xdai') THEN 'gnosis'
        WHEN platform IN ('optimism','optimistic-ethereum') THEN 'optimism'
        WHEN platform IN ('polygon','polygon-pos') THEN 'polygon'
        ELSE NULL
    END AS blockchain, --supported chains only
    provider,
    _inserted_timestamp
FROM all_sources
WHERE token_address IS NOT NULL
    AND blockchain IS NOT NULL
)

SELECT
    token_address,
    id,
    symbol,
    blockchain,
    provider,
     {{ dbt_utils.surrogate_key( 
        ['token_address','id','symbol','blockchain','provider'] ) }} AS _unique_key,
    _inserted_timestamp
FROM FINAL
QUALIFY(ROW_NUMBER() OVER (PARTITION BY token_address, id, symbol, blockchain, provider
    ORDER BY _inserted_timestamp DESC)) = 1