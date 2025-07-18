{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'token_address','price_date'],
    cluster_by = ['blockchain','price_date'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['daily']
) }}
/*
MAJOR Tokens on a chain calculated via prices.

This query finds tokens across chains that closely track ETH or WBTC prices (within 1%)
using Ethereum mainnet prices as the reference.

This supports an Allowlist of highly liquid tokens "imported" to a chain to support liquidity of native tokens.

Token must have BOTH of the following:
2. Average deviation within 1% of either ETH or BTC across the last 1000 hours 
3. Median deviation within 1% of either ETH or BTC across the last 1000 hours 
                */
WITH reference_prices AS (

    SELECT
        HOUR,
        price AS ref_price,
        CASE
            WHEN symbol = 'ETH'
            AND is_native = TRUE THEN 'ETH'
            WHEN token_address = LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599') THEN 'BTC'
        END AS ref_asset
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        blockchain = 'ethereum'
        AND (
            (
                symbol = 'ETH'
                AND is_native = TRUE
            )
            OR token_address = LOWER('0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599')
        ) qualify ROW_NUMBER() over (
            PARTITION BY HOUR,
            ref_asset
            ORDER BY
                HOUR DESC
        ) = 1
),
price_comparisons AS (
    SELECT
        p.hour,
        p.blockchain,
        p.token_address,
        p.price,
        r.ref_price,
        r.ref_asset,
        ABS(
            p.price / r.ref_price - 1
        ) AS price_deviation
    FROM
        {{ ref('price__ez_prices_hourly') }}
        p
        LEFT JOIN reference_prices r
        ON p.hour = r.hour
)
SELECT
    date_day AS price_date,
    blockchain,
    token_address,
    ref_asset AS tracks_asset,
    AVG(price_deviation) AS avg_deviation,
    MEDIAN(price_deviation) AS median_deviation,
    MIN(price_deviation) AS min_deviation,
    MAX(price_deviation) AS max_deviation,
    COUNT(*) AS hours_tracked
FROM
    price_comparisons p
    INNER JOIN {{ ref('core__dim_dates') }}
    d
    ON p.hour >= DATEADD(
        'hour',
        -1000,
        d.date_day
    )
    AND p.hour < d.date_day
WHERE
    token_address IS NOT NULL -- eth on eth?

{% if is_incremental() %}
-- Only process recent dates for incremental runs
AND d.date_day > (
    SELECT
        DATEADD('day', -7, MAX(price_date))
    FROM
        {{ this }})
        AND d.date_day < CURRENT_DATE()
    {% else %}
        AND d.date_day >= '2025-01-01'
        AND d.date_day < CURRENT_DATE()
    {% endif %}
    GROUP BY
        date_day,
        blockchain,
        token_address,
        ref_asset
    HAVING
        avg_deviation BETWEEN 0
        AND 0.01 -- Within 1% of reference
        AND median_deviation BETWEEN 0
        AND 0.01 -- Within 1% of reference
