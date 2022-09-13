{{ config(
    materialized = 'view',
) }}


WITH base_cg_metadata AS (
    SELECT
        DISTINCT id,
        NAME,
        symbol
    FROM
        crosschain_dev.silver.asset_metadata_coin_gecko
)
SELECT
    'coingecko' AS provider,
    p.id,
    p.recorded_hour,
    m.name,
    m.symbol,
    p.open,
    p.high,
    p.low,
    p.close
FROM
    {{ ref('silver__hourly_prices_coin_gecko') }}
    p
    LEFT OUTER JOIN base_cg_metadata m
    ON m.id = p.id
