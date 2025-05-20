{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain', 'block_day']
) }}

WITH token_activity_history AS (
    SELECT
        address,
        blockchain,
        AVG(tx_count) AS avg_tx,
        AVG(unique_senders) AS avg_senders,
        COUNT(*) AS active_days,
        VARIANCE(tx_count) AS var_tx,
        VARIANCE(unique_senders) AS var_senders,
        MAX(tx_count) AS max_tx,
        MAX(unique_senders) AS max_senders,
        DATEDIFF('day', MIN(block_day), MAX(block_day)) + 1 AS life_days,
        COUNT(*) / NULLIF(DATEDIFF('day', MIN(block_day), MAX(block_day)) + 1, 0) AS consistency,
        MAX(tx_count) / NULLIF(AVG(tx_count), 0) AS tx_spike_ratio,
        MAX(tx_count) / NULLIF(SUM(tx_count), 0) AS tx_skew_ratio
    FROM 
        {{ ref('silver__transfers_summary') }}
    GROUP BY 
        address, blockchain
    HAVING 
        active_days >= 5
),

token_activity_percentiles AS (
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY blockchain ORDER BY avg_tx), 2) AS tx_pctl,
        ROUND(PERCENT_RANK() OVER (PARTITION BY blockchain ORDER BY avg_senders), 2) AS senders_pctl,
        ROUND(PERCENT_RANK() OVER (PARTITION BY blockchain ORDER BY consistency), 2) AS consistency_pctl,
        ROUND(PERCENT_RANK() OVER (PARTITION BY blockchain ORDER BY life_days), 2) AS longevity_pctl
    FROM token_activity_history
),
known_tokens AS (
    SELECT
        address,
        blockchain,
        symbol,
        decimals,
        name
    FROM 
        {{ ref('silver__tokens') }}
    where 
        symbol is not null
        or decimals is not null
        or name is not null
),


transfers_today AS (
    SELECT
        d.block_day,
        d.address,
        d.blockchain,
        d.tx_count,
        d.unique_senders,
        d.amount,
        COALESCE(t1.symbol, t2.symbol) as symbol,
        COALESCE(t1.decimals, t2.decimals) as decimals,
        COALESCE(t1.name, t2.name) as name,
        CASE 
            WHEN t1.address IS NOT NULL THEN FALSE
            WHEN t2.address IS NOT NULL THEN TRUE
            ELSE NULL 
        END as is_imputed_token,
        ROUND(PERCENT_RANK() OVER (PARTITION BY d.blockchain, d.block_day ORDER BY tx_count), 2) AS tx_daily_pctl,
        ROUND(PERCENT_RANK() OVER (PARTITION BY d.blockchain, d.block_day ORDER BY unique_senders), 2) AS senders_daily_pctl
    FROM {{ ref('silver__transfers_summary') }} d
    LEFT JOIN known_tokens t1
        ON d.address = t1.address
        AND d.blockchain = t1.blockchain
    LEFT JOIN known_tokens t2
        ON d.address = t2.address
        AND t1.address IS NULL -- Only try the fallback if the first join failed
    {% if is_incremental() %}
    WHERE d.block_day > (SELECT MAX(block_day) FROM {{ this }})
    {% endif %}
),

joined AS (
    SELECT
        t.block_day,
        t.address,
        t.blockchain,
        t.symbol,
        t.decimals,
        t.name,
        t.tx_count,
        t.unique_senders,
        t.amount,
        t.tx_daily_pctl,
        t.senders_daily_pctl,

        h.avg_tx,
        h.avg_senders,
        h.active_days,
        h.consistency,
        h.life_days,
        h.tx_spike_ratio,
        h.tx_skew_ratio,

        h.tx_pctl,
        h.senders_pctl,
        h.consistency_pctl,
        h.longevity_pctl,
        ROUND(
            (
                (h.tx_pctl * 0.25) + --How active the token is in terms of raw transaction count, relative to others on the same chain
                (h.senders_pctl * 0.25) + --How widely used it is (number of unique senders)
                (h.consistency_pctl * 0.25) + --How consistently active it is day-to-day (vs. bursty or dormant)
                (h.longevity_pctl * 0.15) + --How long the token has been consistently active (sustained presence)
                ((1 - LEAST(h.tx_spike_ratio / 10, 1)) * 0.10) --Penalizes tokens with short-lived activity spikes (e.g., airdrops or wash trading)
            )
        , 3) AS legitimacy_score,
        CASE
            WHEN (h.tx_spike_ratio > 10 AND h.active_days < 10) THEN 'spike_anomaly'
            ELSE 'normal'
        END AS anomaly_flag
    FROM transfers_today t
    LEFT JOIN token_activity_percentiles h
        ON t.address = h.address AND t.blockchain = h.blockchain
)

SELECT 
    *,
    CASE
        WHEN blockchain = 'aleo' AND legitimacy_score > 0.7 THEN 'verified'
        WHEN blockchain = 'aptos' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'arbitrum' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'avalanche' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'axelar' AND legitimacy_score > 0.09 THEN 'verified'
        WHEN blockchain = 'base' AND legitimacy_score > 0.90 THEN 'verified'
        WHEN blockchain = 'blast' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'boba' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'bsc' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'core' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'cosmos' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'eclipse' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'ethereum' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'flow' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'gnosis' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'ink' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'kaia' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'mantle' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'near' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'optimism' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'osmosis' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'polygon' AND legitimacy_score > 0.9 THEN 'verified'
        WHEN blockchain = 'sei' AND legitimacy_score > 0.8 THEN 'verified'
        WHEN blockchain = 'solana' AND legitimacy_score > 0.90 THEN 'verified'
        ELSE 'not_verified'
    END AS chain_adjusted_verification_status
FROM joined
QUALIFY ROW_NUMBER() OVER (PARTITION BY address, blockchain, block_day ORDER BY block_day DESC) = 1