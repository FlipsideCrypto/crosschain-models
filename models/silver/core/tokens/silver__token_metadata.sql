{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain', 'block_day'],
    tags = ['daily']
) }}

WITH daily_metrics AS (
    SELECT
        d.block_day,
        d.address,
        REPLACE(d.blockchain,'_evm') as blockchain,
        d.tx_count,
        d.unique_senders,
        d.amount,
        -- Calculate daily percentiles
        ROUND(CUME_DIST() OVER (PARTITION BY d.blockchain, d.block_day ORDER BY tx_count), 2) AS tx_daily_pctl,
        ROUND(CUME_DIST() OVER (PARTITION BY d.blockchain, d.block_day ORDER BY unique_senders), 2) AS senders_daily_pctl,
        -- Calculate rolling metrics up to current day
        AVG(d.tx_count) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_tx,
        AVG(d.unique_senders) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS avg_senders,
        COUNT(*) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS active_days,
        VARIANCE(d.tx_count) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS var_tx,
        VARIANCE(d.unique_senders) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS var_senders,
        MAX(d.tx_count) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS max_tx,
        MAX(d.unique_senders) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS max_senders,
        ROW_NUMBER() OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day
        ) AS life_days,
        -- Calculate max tx percentile
        MAX(ROUND(CUME_DIST() OVER (PARTITION BY d.blockchain, d.block_day ORDER BY tx_count), 2)) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS max_tx_pctl,
        -- Calculate avg tx percentile
        ROUND(CUME_DIST() OVER (
            PARTITION BY d.blockchain, d.block_day 
            ORDER BY AVG(d.tx_count) OVER (
                PARTITION BY d.address, d.blockchain 
                ORDER BY d.block_day 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ), 2) AS avg_tx_pctl,
        MAX(d.tx_count) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / NULLIF(
            SUM(d.tx_count) OVER (
                PARTITION BY d.address, d.blockchain 
                ORDER BY d.block_day 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 0
        ) AS tx_skew_ratio
    FROM {{ ref('silver__transfers_summary') }} d
    {% if is_incremental() %}
    WHERE d.block_day > (SELECT MAX(block_day) FROM {{ this }})
    {% endif %}
),

daily_percentiles AS (
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY blockchain, block_day ORDER BY avg_tx), 2) AS tx_pctl,
        ROUND(CUME_DIST() OVER (PARTITION BY blockchain, block_day ORDER BY avg_senders), 2) AS senders_pctl,
        ROUND(CUME_DIST() OVER (PARTITION BY blockchain, block_day ORDER BY life_days), 2) AS longevity_pctl,
        -- Calculate spike ratio based on percentiles
        NULLIF(max_tx_pctl, 0) / NULLIF(avg_tx_pctl, 0) AS tx_spike_ratio
    FROM daily_metrics
    WHERE active_days >= 5
)

SELECT 
    *,
    ROUND(
        (
            (tx_pctl * 0.40) + --How active the token is in terms of raw transaction count, relative to others on the same chain
            (senders_pctl * 0.40) + --How widely used it is (number of unique senders)
            (longevity_pctl * 0.10) + --How long the token has been consistently active (sustained presence)
            ((1 - LEAST(tx_spike_ratio / 10, 1)) * 0.10) --Penalizes tokens with short-lived activity spikes (e.g., airdrops or wash trading)
        )
    , 3) AS legitimacy_score,
    CASE
        WHEN (tx_spike_ratio > 10 AND active_days < 10) THEN 'spike_anomaly'
        ELSE 'normal'
    END AS anomaly_flag,
    CASE
        WHEN blockchain = 'aleo' AND legitimacy_score > 0.7 THEN TRUE
        WHEN blockchain = 'aptos' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'arbitrum' AND legitimacy_score > 0.92 THEN TRUE
        WHEN blockchain = 'avalanche' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'axelar' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'base' AND legitimacy_score > 0.95 THEN TRUE
        WHEN blockchain = 'blast' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'bob' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'boba' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'bsc' AND legitimacy_score > 0.94 THEN TRUE
        WHEN blockchain = 'core' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'cosmos' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'eclipse' AND legitimacy_score > 0.95 THEN TRUE
        WHEN blockchain = 'ethereum' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'flow' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'gnosis' AND legitimacy_score > 0.93 THEN TRUE
        WHEN blockchain = 'ink' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'kaia' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'mantle' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'maya' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'near' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'optimism' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'osmosis' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'polygon' AND legitimacy_score > 0.94 THEN TRUE
        WHEN blockchain = 'ronin' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'sei' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'solana' AND legitimacy_score > 0.90 THEN TRUE
        WHEN blockchain = 'stellar' AND legitimacy_score > 0.95 THEN TRUE
        WHEN blockchain = 'swell' AND legitimacy_score > 0.90 THEN TRUE
        WHEN blockchain = 'thorchain' AND legitimacy_score > 0.90 THEN TRUE
        WHEN blockchain = 'ton' AND legitimacy_score > 0.92 THEN TRUE
        ELSE FALSE
    END AS is_verified
FROM daily_percentiles