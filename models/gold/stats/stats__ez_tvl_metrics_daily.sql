{{ config(
    materialized = 'view',
    tags = ['metrics_daily']
) }}

WITH source_data AS (
    -- Using daily data directly

    SELECT
        DATE :: DATE AS DATE,
        chain,
        SPLIT_PART(
            chain,
            '-',
            1
        ) AS clean_chain,
        tvl_usd AS daily_tvl
    FROM
        EXTERNAL.defillama.fact_chain_tvl
    WHERE
        1 = 1 -- Need at least 90 days of history for rolling calcs + current day
        {# and DATE BETWEEN CURRENT_DATE() - 90
        AND CURRENT_DATE() #}
        AND clean_chain NOT IN (
            'dcAndLsOverlap',
            'liquidstaking',
            'doublecounted',
            'borrowed'
        )
),
tvl_metrics AS (
    SELECT
        DATE,
        clean_chain AS chain,
        daily_tvl AS current_tvl,
        -- Lagged TVL values for daily change calculations
        LAG(
            daily_tvl,
            1
        ) over (
            PARTITION BY clean_chain
            ORDER BY
                DATE
        ) AS prev_1_day_tvl,
        LAG(
            daily_tvl,
            7
        ) over (
            PARTITION BY clean_chain
            ORDER BY
                DATE
        ) AS prev_7_day_tvl,
        LAG(
            daily_tvl,
            30
        ) over (
            PARTITION BY clean_chain
            ORDER BY
                DATE
        ) AS prev_30_day_tvl,
        LAG(
            daily_tvl,
            90
        ) over (
            PARTITION BY clean_chain
            ORDER BY
                DATE
        ) AS prev_90_day_tvl,
        -- Calculate rolling volatility (90-day standard deviation)
        STDDEV(daily_tvl) over (
            PARTITION BY clean_chain
            ORDER BY
                DATE rows BETWEEN 89 preceding
                AND CURRENT ROW
        ) AS tvl_volatility_90d,
        -- Calculate rolling max drawdown (90-day)
        MAX(daily_tvl) over (
            PARTITION BY clean_chain
            ORDER BY
                DATE rows BETWEEN 89 preceding
                AND CURRENT ROW
        ) AS rolling_90d_max_tvl,
        MIN(daily_tvl) over (
            PARTITION BY clean_chain
            ORDER BY
                DATE rows BETWEEN 89 preceding
                AND CURRENT ROW
        ) AS rolling_90d_min_tvl,
        -- Calculate 14-day rolling average TVL for ranking
        AVG(daily_tvl) over (
            PARTITION BY clean_chain
            ORDER BY
                DATE rows BETWEEN 13 preceding
                AND CURRENT ROW
        ) AS rolling_14d_avg_tvl
    FROM
        source_data
)
SELECT
    DATE AS block_date,
    LOWER(chain) AS blockchain,
    current_tvl,
    current_tvl - prev_1_day_tvl AS day_1_change,
    -- Absolute daily changes
    ROUND((day_1_change / NULLIF(prev_1_day_tvl, 0)) * 100, 2) AS pct_change_1d,
    -- Percentage changes over different periods
    ROUND((tvl_volatility_90d / NULLIF(current_tvl, 0)) * 100, 2) AS volatility_score_90d,
    -- Rolling 90-day volatility score (normalized by current TVL)
    ROUND(
        ((rolling_90d_min_tvl - rolling_90d_max_tvl) / NULLIF(rolling_90d_max_tvl, 0)) * 100,
        2
    ) AS max_drawdown_pct_90d -- Rolling 90-day maximum drawdown percentage,,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain','a.block_date']) }} AS ez_stablecoin_flows_daily_id,
    MAX(inserted_timestamp) AS inserted_timestamp,
    MAX(modified_timestamp) AS modified_timestamp
FROM
    tvl_metrics
