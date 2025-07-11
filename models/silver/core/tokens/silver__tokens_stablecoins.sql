{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'token_address','price_date'],
    cluster_by = ['blockchain','price_date'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['daily']
) }}
/*
This query finds stablecoins across chains that closely track USD (within 2%).

Token must have ALL THREE of the following:
2. Average deviation within 2% of $1 across the last 1000 hours 
3. Median deviation within 2% of $1 across the last 1000 hours 
*/
WITH price_comparisons AS (

    SELECT
        HOUR,
        blockchain,
        token_address,
        price,
        ABS(
            price - 1
        ) AS usd_deviation
    FROM
        {{ ref('price__ez_prices_hourly') }}
)
SELECT
    date_day AS price_date,
    blockchain,
    token_address,
    AVG(usd_deviation) AS avg_deviation,
    MEDIAN(usd_deviation) AS median_deviation,
    MIN(usd_deviation) AS min_deviation,
    MAX(usd_deviation) AS max_deviation,
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
    token_address != '0x53fc82f14f009009b440a706e31c9021e1196a2f' -- BUIDL on avax?

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
        token_address
    HAVING
        avg_deviation BETWEEN 0
        AND 0.02 -- Within 2% of $1
        AND median_deviation BETWEEN 0
        AND 0.02 -- Median within 2%
