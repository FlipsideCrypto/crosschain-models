{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['hour::DATE'],
) }}

WITH coin_gecko_meta AS (
    SELECT
        DISTINCT REGEXP_SUBSTR(REGEXP_REPLACE(token_address, '^x', '0x'), '0x[a-zA-Z0-9]*') AS token_address,
        id,
        symbol,
        LOWER(platform :: STRING) AS platform,
        'coingecko' AS provider
    FROM
        {{ ref(
            'silver__asset_metadata_coin_gecko'
        ) }} 
{% if is_incremental() %}
AND
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
        id,
        symbol,
        LOWER(platform :: STRING) AS platform,
        'coinmarketcap' AS provider
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
        m.token_address,
        p.asset_id AS id,
        COALESCE(p.symbol,m.symbol) AS symbol,
        LOWER(p.platform :name :: STRING) AS platform,
        'coingecko' AS provider
    FROM
        {{ source(
            'legacy_db',
            'prices_v2'
        ) }} p
        LEFT JOIN coin_gecko_meta m
        ON m.id = p.asset_id
    WHERE
        provider = 'coingecko'
        AND recorded_at :: DATE < '2022-08-24'
{% if is_incremental() %}
AND
    token_address NOT IN (
        SELECT 
            DISTINCT token_address
        FROM {{ this }}
    )
{% endif %}
),

legacy_coin_market_cap_meta AS (
    SELECT
        m.token_address,
        p.asset_id AS id,
        COALESCE(p.symbol,m.symbol) AS symbol,
        LOWER(p.platform :name :: STRING) AS platform,
        'coinmarketcap' AS provider
    FROM
        {{ source(
            'legacy_db',
            'prices_v2'
        ) }} p
        LEFT JOIN coin_market_cap_meta m
        ON m.id = p.asset_id
    WHERE
        provider = 'coinmarketcap'
        AND recorded_at :: DATE < '2022-07-20'
{% if is_incremental() %}
AND
    token_address NOT IN (
        SELECT 
            DISTINCT token_address
        FROM {{ this }}
    )
{% endif %}
),

FINAL AS (
    SELECT
        token_address,
        id,
        symbol,
        platform,
        provider
    FROM
        coin_gecko_meta
    UNION ALL
    SELECT
        token_address,
        id,
        symbol,
        platform,
        provider
    FROM
        coin_market_cap_meta
    UNION ALL
    SELECT
        token_address,
        id,
        symbol,
        platform,
        provider
    FROM
        legacy_coin_gecko_meta
    UNION ALL
    SELECT
        token_address,
        id,
        symbol,
        platform,
        provider
    FROM
        legacy_coin_market_cap_meta
)

SELECT
    token_address,
    id,
    UPPER(COALESCE(c.symbol,f.symbol)) AS symbol,
    c.decimals,
    platform AS blockchain,
    provider
FROM FINAL f 
LEFT JOIN {{ ref('core__dim_contracts') }} c 
    ON LOWER(c.address) = LOWER(p.token_address)