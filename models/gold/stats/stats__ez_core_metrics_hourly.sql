{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } }
) }}

SELECT
    s.blockchain,
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_initiator_count,
    ROUND(
        total_fees_native * p.price,
        2
    ) AS total_fees_usd,
    core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_hourly') }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.blockchain = (
        CASE
            WHEN s.blockchain IN (
                'arbitrum',
                'base',
                'optimism'
            ) THEN 'ethereum' --use ethereum WETH on L2s for better coverage
            ELSE s.blockchain
        END
    )
    AND p.token_address = (
        CASE
            WHEN s.blockchain = 'arbitrum' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
            WHEN s.blockchain = 'avalanche' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' --WAVAX
            WHEN s.blockchain = 'base' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
            WHEN s.blockchain = 'bsc' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' --WBNB
            WHEN s.blockchain = 'ethereum' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
            WHEN s.blockchain = 'gnosis' THEN '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' --WXDAI
            WHEN s.blockchain = 'optimism' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
            WHEN s.blockchain = 'polygon' THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' --WMATIC
        END
    )
