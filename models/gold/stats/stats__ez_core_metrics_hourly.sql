{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } }
) }}

SELECT
    blockchain,
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_initiator_count,
    total_fees_usd,
    core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_hourly') }}
