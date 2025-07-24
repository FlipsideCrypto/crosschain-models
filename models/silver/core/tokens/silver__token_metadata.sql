{{ config(
    materialized = 'incremental',
    unique_key = ['token_metadata_id'],
    tags = ['daily']
) }}

WITH daily_metrics AS (
    SELECT
        d.block_day,
        d.address,
        REPLACE(d.blockchain,'_evm') as blockchain,
        c.symbol,
        c.NAME,
        c.decimals,
        COALESCE(c.created_block_timestamp, MIN(d.block_day) OVER (
            PARTITION BY d.address, d.blockchain 
            ORDER BY d.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) as first_block_timestamp,
        d.tx_count,
        d.unique_senders,
        d.amount,
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
        DATEDIFF('day', DATE(c.created_block_timestamp), d.block_day) as life_days,
    FROM {{ ref('silver__transfers_summary') }} d
        LEFT JOIN {{ ref('silver__contracts') }} c
        ON d.address = c.address
        AND d.blockchain = c.blockchain
    {% if is_incremental() %}
    WHERE d.block_day > (SELECT MAX(block_day) FROM {{ this }})
    {% endif %}
),

daily_percentiles AS (
    SELECT
        m.*,
        -- Calculate daily percentiles
        ROUND(CUME_DIST() OVER (PARTITION BY m.blockchain, m.block_day ORDER BY m.tx_count), 2) AS tx_daily_pctl,
        ROUND(CUME_DIST() OVER (PARTITION BY m.blockchain, m.block_day ORDER BY m.unique_senders), 2) AS senders_daily_pctl,
        ROUND(CUME_DIST() OVER (PARTITION BY m.blockchain, m.block_day ORDER BY m.avg_tx), 2) AS tx_pctl,
        ROUND(CUME_DIST() OVER (PARTITION BY m.blockchain, m.block_day ORDER BY m.avg_senders), 2) AS senders_pctl,
        ROUND(CUME_DIST() OVER (PARTITION BY m.blockchain, m.block_day ORDER BY m.life_days), 2) AS longevity_pctl
    FROM daily_metrics m
    WHERE m.active_days >= 5
),

spike_metrics AS (
    SELECT
        p.*,
        -- Calculate max tx percentile for each token
        MAX(p.tx_daily_pctl) OVER (
            PARTITION BY p.address, p.blockchain 
            ORDER BY p.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS max_tx_pctl,
        -- Calculate the ratio of max to avg percentile
        NULLIF(MAX(p.tx_daily_pctl) OVER (
            PARTITION BY p.address, p.blockchain 
            ORDER BY p.block_day 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 0) / NULLIF(p.tx_pctl, 0) AS spike_ratio
    FROM daily_percentiles p
),

final_metrics AS (
    SELECT
        s.*,
        -- Get percentile of the spike ratio
        ROUND(CUME_DIST() OVER (
            PARTITION BY s.blockchain, s.block_day 
            ORDER BY s.spike_ratio
        ), 2) AS tx_spike_ratio
    FROM spike_metrics s
)

SELECT 
    block_day,
    address,
    blockchain,
    symbol,
    NAME,
    decimals,
    first_block_timestamp,
    tx_count,
    unique_senders,
    ROUND(amount, 2) as amount,
    ROUND(avg_tx, 2) as avg_tx,
    ROUND(avg_senders, 2) as avg_senders,
    active_days,
    life_days,
    tx_daily_pctl,
    senders_daily_pctl,
    tx_spike_ratio as tx_spike_ratio_penalty,
    tx_pctl,
    senders_pctl,
    longevity_pctl,
    ROUND(
        (
            (tx_daily_pctl * 0.30) + --How active the token is in terms of raw transaction count, relative to others on the same chain
            (senders_daily_pctl * 0.30) + --How widely used it is (number of unique senders)
            (longevity_pctl * 0.20) + --How long the token has been consistently active (sustained presence)
            ((1 - LEAST(tx_spike_ratio / 10, 1)) * 0.20) --Penalizes tokens with short-lived activity spikes (e.g., airdrops or wash trading)
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
        WHEN blockchain = 'maya' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'near' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'optimism' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'osmosis' AND legitimacy_score > 0.8 THEN TRUE
        WHEN blockchain = 'polygon' AND legitimacy_score > 0.94 THEN TRUE
        WHEN blockchain = 'ronin' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'sei' AND legitimacy_score > 0.9 THEN TRUE
        WHEN blockchain = 'solana' AND legitimacy_score > 0.90 THEN TRUE
        WHEN blockchain = 'stellar' AND legitimacy_score > 0.95 THEN TRUE
        WHEN blockchain = 'thorchain' AND legitimacy_score > 0.90 THEN TRUE
        WHEN blockchain = 'ton' AND legitimacy_score > 0.92 THEN TRUE
        ELSE FALSE
    END AS is_verified,
    {{ dbt_utils.generate_surrogate_key(['block_day','address','blockchain' ]) }} AS token_metadata_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    final_metrics f
